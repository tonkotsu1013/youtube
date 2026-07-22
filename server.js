const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// ↓ この1行を追加します！
app.use(express.static(path.join(__dirname)));

// データベース初期化
const db = new sqlite3.Database('./database.sqlite', (err) => {
    if (err) console.error('Database connection error:', err.message);
    else console.log('Connected to the SQLite database.');
});

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE, name TEXT UNIQUE)`);
    db.run(`CREATE TABLE IF NOT EXISTS friends (user_code TEXT, friend_code TEXT, status TEXT, is_favorite INTEGER DEFAULT 0, PRIMARY KEY (user_code, friend_code))`); 
    db.run(`CREATE TABLE IF NOT EXISTS match_history (id INTEGER PRIMARY KEY AUTOINCREMENT, p1_code TEXT, p2_code TEXT, p1_type TEXT, p2_type TEXT, date DATETIME DEFAULT CURRENT_TIMESTAMP)`);
    // 勝敗記録用のカラムを追加
    db.run(`ALTER TABLE match_history ADD COLUMN winner_code TEXT`, (err) => {}); 
    // 対戦を一意に特定するためのキー（サーバー内部のmatchId文字列）。
    // これがないと「p1_code/p2_code一致 かつ winner_code IS NULL の最新行」という
    // あいまいな検索に頼ることになり、同じ相手と連戦した際に別の試合の勝敗を
    // 誤って上書きしてしまうことがあった。
    db.run(`ALTER TABLE match_history ADD COLUMN match_key TEXT`, (err) => {});
db.run(`CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, sender_code TEXT, receiver_code TEXT, group_id INTEGER, message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)`);
db.run(`CREATE TABLE IF NOT EXISTS chat_groups (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, owner_code TEXT)`);
    db.run(`CREATE TABLE IF NOT EXISTS group_members (group_id INTEGER, user_code TEXT, PRIMARY KEY (group_id, user_code))`);

    // ===== 世界ランキング機能用カラム追加 =====
    db.run(`ALTER TABLE users ADD COLUMN rating INTEGER DEFAULT 1200`, (err) => {});
    db.run(`ALTER TABLE users ADD COLUMN wins INTEGER DEFAULT 0`, (err) => {});
    db.run(`ALTER TABLE users ADD COLUMN losses INTEGER DEFAULT 0`, (err) => {});
    db.run(`ALTER TABLE users ADD COLUMN win_streak INTEGER DEFAULT 0`, (err) => {});
    db.run(`ALTER TABLE users ADD COLUMN best_streak INTEGER DEFAULT 0`, (err) => {});

    // ===== どのモードのスコアでも登録できる「世界記録」テーブル =====
    // mode = ゲームモード名（marathon, time, puyo_marathon 等）、code = ユーザーの固有コード
    // 1人1モードにつき自己ベストの記録のみを保持する（PRIMARY KEYで自動的に一意化）
    db.run(`CREATE TABLE IF NOT EXISTS world_records (
        mode TEXT,
        code TEXT,
        name TEXT,
        value REAL,
        score INTEGER DEFAULT 0,
        lines INTEGER DEFAULT 0,
        time_ms INTEGER DEFAULT 0,
        lower_is_better INTEGER DEFAULT 0,
        date DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (mode, code)
    )`);
});

// ===== Promise版のDBヘルパー（世界記録登録処理をシンプルに書くため） =====
function dbGet(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.get(sql, params, (err, row) => err ? reject(err) : resolve(row));
    });
}
function dbAll(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows));
    });
}
function dbRun(sql, params = []) {
    return new Promise((resolve, reject) => {
        db.run(sql, params, function (err) { err ? reject(err) : resolve(this); });
    });
}

// ===== レーティング(ELO)計算＆更新 =====
// 対戦結果を受けて勝者・敗者のレーティング、勝敗数、連勝記録を更新し、本人にだけ変動値を通知する
function updateRatingsAndRecord(winnerCode, loserCode) {
    if (!winnerCode || !loserCode || winnerCode === loserCode) return;
    db.get(`SELECT rating, win_streak, best_streak FROM users WHERE code = ?`, [winnerCode], (err, winner) => {
        db.get(`SELECT rating FROM users WHERE code = ?`, [loserCode], (err2, loser) => {
            if (!winner || !loser) return;
            const K = 32;
            const rw = winner.rating != null ? winner.rating : 1200;
            const rl = loser.rating != null ? loser.rating : 1200;
            const expectedW = 1 / (1 + Math.pow(10, (rl - rw) / 400));
            const newRw = Math.round(rw + K * (1 - expectedW));
            const newRl = Math.round(rl - K * (1 - expectedW));
            const newStreak = (winner.win_streak || 0) + 1;
            const newBest = Math.max(winner.best_streak || 0, newStreak);

            db.run(`UPDATE users SET rating = ?, wins = wins + 1, win_streak = ?, best_streak = ? WHERE code = ?`,
                [newRw, newStreak, newBest, winnerCode]);
            db.run(`UPDATE users SET rating = ?, losses = losses + 1, win_streak = 0 WHERE code = ?`,
                [newRl, loserCode]);

            const winnerSocketId = socketMap[winnerCode];
            const loserSocketId = socketMap[loserCode];
            if (winnerSocketId) io.to(winnerSocketId).emit('rating_update', { rating: newRw, delta: newRw - rw, streak: newStreak });
            if (loserSocketId) io.to(loserSocketId).emit('rating_update', { rating: newRl, delta: newRl - rl, streak: 0 });
        });
    });
}

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

const onlineUsers = {}; 
const socketMap = {};   
let matchmakingQueue = [];
const activeMatches = {}; 

const RECONNECT_GRACE_MS = 20000; // 切断してもすぐには敗北にせず、再接続を待つ猶予時間(ms)

// 対戦を一つの結果として確定させる共通処理。
// matchId・winnerCode・loserCodeを渡すだけで、DB更新・レーティング更新・
// 両者への通知・ステータスリセット・activeMatchesの後片付けまで一括で行う。
// game_over / return_to_lobby / disconnect(猶予後) の3箇所から呼ばれる。
function recordMatchResult(matchId, match, winnerCode, loserCode, winReason, loseReason) {
    if (!match || match.gameOverHandled) return;
    match.gameOverHandled = true;

    const winnerSocketId = socketMap[winnerCode];
    const loserSocketId = socketMap[loserCode];

    // match_key（matchId文字列そのもの）で一意に対戦を特定してUPDATEするので、
    // 同じ相手と連戦した場合でも別の試合の結果を誤って上書きすることがない。
    db.run(`UPDATE match_history SET winner_code = ? WHERE match_key = ?`, [winnerCode, matchId], (err) => {
        if (err) console.error('UPDATE winner_code error:', err);
    });

    updateRatingsAndRecord(winnerCode, loserCode);

    if (winnerSocketId) {
        io.to(winnerSocketId).emit('game_result', { result: 'win', reason: winReason });
        if (onlineUsers[winnerSocketId]) onlineUsers[winnerSocketId].status = 'idle';
    }
    if (loserSocketId) {
        io.to(loserSocketId).emit('game_result', { result: 'lose', reason: loseReason });
        if (onlineUsers[loserSocketId]) onlineUsers[loserSocketId].status = 'idle';
    }

    delete activeMatches[matchId];
    io.emit('friends_data_update');
}

io.on('connection', (socket) => {
// 1. 厳密なログイン処理
    socket.on('login', (data, callback) => {
        const { code, name } = data;
        
        if (onlineUsers[socket.id]) {
            delete socketMap[onlineUsers[socket.id].code];
            delete onlineUsers[socket.id];
        }

        db.get(`SELECT * FROM users WHERE code = ? OR name = ?`, [code, name], (err, row) => {
            if (row) {
                // ① データベースに一致するユーザーが見つかった場合
                if (row.code === code && row.name === name) {
                    // --- 以下の3行を追加 ---
                    if (socketMap[code] && socketMap[code] !== socket.id) {
                        delete onlineUsers[socketMap[code]];
                    }
                    // -----------------------
                    setupUserStatus(socket.id, code, name);
                    callback({ success: true, message: 'ログインしました' });
                } else {
                    // 名前かコードのどちらかが、他の誰かに既に使われている場合
                    callback({ success: false, message: '指定されたコードまたは名前は既に使用されています' });
                }
            } else {
                // ② データベースにデータがなかった場合（新規登録）
                db.run(`INSERT INTO users (code, name) VALUES (?, ?)`, [code, name], function(err) {
                    if (err) return callback({ success: false, message: '登録に失敗しました' });
                    setupUserStatus(socket.id, code, name);
                    callback({ success: true, message: '新規登録＆ログインしました' });
                });
            }
        });
    });

    function setupUserStatus(socketId, code, name) {
        onlineUsers[socketId] = { code, name, status: 'idle' };
        socketMap[code] = socketId;

        // 再接続の場合、まだ進行中のマッチに参加していればステータスを復元する
        for (const mId in activeMatches) {
            const m = activeMatches[mId];
            if ((m.p1 === code || m.p2 === code) && !m.gameOverHandled) {
                onlineUsers[socketId].status = 'playing';
                break;
            }
        }

        io.emit('friends_data_update'); 
        io.emit('online_count', Object.keys(socketMap).length);
    }

    // 2. フレンド取得
    socket.on('get_friends', () => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        
        db.all(`
            SELECT u.code, u.name, u.rating, u.wins, u.losses, f.status, f.is_favorite 
            FROM friends f 
            JOIN users u ON (f.friend_code = u.code AND f.user_code = ?) 
                         OR (f.user_code = u.code AND f.friend_code = ? AND f.status = 'pending')
        `, [user.code, user.code], (err, rows) => {
            if (err) return;
            const friendsList = rows.map(r => ({
                code: r.code, name: r.name, status: r.status, is_favorite: r.is_favorite,
                rating: r.rating != null ? r.rating : 1200, wins: r.wins || 0, losses: r.losses || 0,
                isOnline: !!socketMap[r.code],
                currentActivity: socketMap[r.code] ? onlineUsers[socketMap[r.code]].status : 'offline'
            }));
            socket.emit('friends_data', friendsList);
        });
    });

    // ===== どのモードでも記録を世界ランキングに登録 =====
    // data: { mode, value, score, lines, timeMs, lowerIsBetter }
    // value: 比較に使う数値（スコア系は高いほど良い / タイムアタック系は低いほど良い）
    socket.on('register_world_record', async (data, callback) => {
        try {
            const user = onlineUsers[socket.id];
            if (!user) return callback && callback({ success: false, message: 'ログインしてから登録してください。' });

            const { mode, value, score, lines, timeMs, lowerIsBetter } = data || {};
            if (!mode || typeof value !== 'number' || !isFinite(value)) {
                return callback && callback({ success: false, message: '記録データが不正です。' });
            }
            const dir = lowerIsBetter ? 1 : 0;

            const existing = await dbGet(`SELECT value FROM world_records WHERE mode = ? AND code = ?`, [mode, user.code]);
            const isBetter = !existing || (lowerIsBetter ? value < existing.value : value > existing.value);

            if (isBetter) {
                await dbRun(`INSERT INTO world_records (mode, code, name, value, score, lines, time_ms, lower_is_better, date)
                             VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
                             ON CONFLICT(mode, code) DO UPDATE SET
                                name = excluded.name, value = excluded.value, score = excluded.score,
                                lines = excluded.lines, time_ms = excluded.time_ms,
                                lower_is_better = excluded.lower_is_better, date = CURRENT_TIMESTAMP`,
                    [mode, user.code, user.name, value, score || 0, lines || 0, timeMs || 0, dir]);
            }

            const bestValue = isBetter ? value : existing.value;
            const cmpSql = lowerIsBetter ? 'value < ?' : 'value > ?';
            const betterCount = await dbGet(`SELECT COUNT(*) as cnt FROM world_records WHERE mode = ? AND ${cmpSql}`, [mode, bestValue]);
            const totalCount = await dbGet(`SELECT COUNT(*) as cnt FROM world_records WHERE mode = ?`, [mode]);
            const rank = (betterCount ? betterCount.cnt : 0) + 1;

            callback && callback({ success: true, rank, total: totalCount ? totalCount.cnt : rank, isNewRecord: isBetter, bestValue });
        } catch (e) {
            console.error('register_world_record error:', e);
            callback && callback({ success: false, message: '登録中にエラーが発生しました。' });
        }
    });

    // ===== 特定モードの世界記録ランキングを取得 =====
    socket.on('get_world_ranking', async (data) => {
        try {
            const user = onlineUsers[socket.id];
            const { mode, lowerIsBetter } = data || {};
            if (!mode) return;
            const orderSql = lowerIsBetter ? 'ASC' : 'DESC';

            const top = await dbAll(
                `SELECT code, name, value, score, lines, time_ms, date FROM world_records WHERE mode = ? ORDER BY value ${orderSql} LIMIT 100`,
                [mode]
            );
            const ranked = top.map((r, i) => ({ rank: i + 1, ...r }));

            let myRank = null, myData = null;
            if (user) {
                const me = await dbGet(`SELECT code, name, value, score, lines, time_ms, date FROM world_records WHERE mode = ? AND code = ?`, [mode, user.code]);
                if (me) {
                    const cmpSql = lowerIsBetter ? 'value < ?' : 'value > ?';
                    const betterCount = await dbGet(`SELECT COUNT(*) as cnt FROM world_records WHERE mode = ? AND ${cmpSql}`, [mode, me.value]);
                    myRank = (betterCount ? betterCount.cnt : 0) + 1;
                    myData = me;
                }
            }
            socket.emit('world_ranking_data', { mode, top: ranked, myRank, myData, total: ranked.length });
        } catch (e) {
            console.error('get_world_ranking error:', e);
        }
    });

    // ===== 世界ランキング取得 =====
    socket.on('get_ranking', () => {
        const user = onlineUsers[socket.id];
        db.all(`SELECT code, name, rating, wins, losses, best_streak FROM users ORDER BY rating DESC, wins DESC LIMIT 100`, (err, rows) => {
            const top = (rows || []).map((r, i) => ({ rank: i + 1, code: r.code, name: r.name, rating: r.rating != null ? r.rating : 1200, wins: r.wins || 0, losses: r.losses || 0, best_streak: r.best_streak || 0 }));
            const onlineCount = Object.keys(socketMap).length;

            const respond = (myRank, myData) => socket.emit('ranking_data', { top, myRank, myData, onlineCount });

            if (!user) return respond(null, null);
            db.get(`SELECT code, name, rating, wins, losses, best_streak FROM users WHERE code = ?`, [user.code], (err2, me) => {
                if (!me) return respond(null, null);
                const rating = me.rating != null ? me.rating : 1200;
                db.get(`SELECT COUNT(*) as cnt FROM users WHERE rating > ?`, [rating], (err3, cntRow) => {
                    const myRank = (cntRow ? cntRow.cnt : 0) + 1;
                    respond(myRank, { code: me.code, name: me.name, rating, wins: me.wins || 0, losses: me.losses || 0, best_streak: me.best_streak || 0 });
                });
            });
        });
    });

    // ===== 対戦中のリアクション（絵文字）送信 =====
    socket.on('send_emote', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        const { matchId, emote } = data || {};
        const match = activeMatches[matchId];
        if (!match || (match.p1 !== user.code && match.p2 !== user.code)) return;
        const targetCode = (match.p1 === user.code) ? match.p2 : match.p1;
        const targetSocketId = socketMap[targetCode];
        if (targetSocketId) io.to(targetSocketId).emit('receive_emote', { emote, from: user.name });
    });

    socket.on('toggle_favorite', (targetCode) => {
        const user = onlineUsers[socket.id];
        if(!user) return;
        db.run(`UPDATE friends SET is_favorite = CASE WHEN is_favorite = 1 THEN 0 ELSE 1 END WHERE user_code = ? AND friend_code = ?`, [user.code, targetCode], () => {
            socket.emit('friends_data_update');
        });
    });

    socket.on('delete_friend', (targetCode) => {
        const user = onlineUsers[socket.id];
        if(!user) return;
        db.run(`DELETE FROM friends WHERE (user_code = ? AND friend_code = ?) OR (user_code = ? AND friend_code = ?)`, [user.code, targetCode, targetCode, user.code], () => {
            socket.emit('friends_data_update');
            if (socketMap[targetCode]) io.to(socketMap[targetCode]).emit('friends_data_update');
        });
    });

    socket.on('get_history', (targetCode) => {
        const user = onlineUsers[socket.id];
        if(!user) return;
        db.all(`SELECT * FROM match_history WHERE (p1_code = ? AND p2_code = ?) OR (p1_code = ? AND p2_code = ?) ORDER BY date DESC LIMIT 15`, 
        [user.code, targetCode, targetCode, user.code], (err, rows) => {
            socket.emit('history_data', { targetCode, history: rows || [] });
        });
    });

    // 3. フレンド申請系
    socket.on('send_friend_request', (targetCode, callback) => {
        const user = onlineUsers[socket.id];
        if (!user || user.code === targetCode) return callback({ success: false, message: '無効な操作です。' });
        db.get(`SELECT * FROM users WHERE code = ?`, [targetCode], (err, row) => {
            if (!row) return callback({ success: false, message: 'ユーザーが見つかりません。' });
            db.run(`INSERT INTO friends (user_code, friend_code, status) VALUES (?, ?, 'pending')`, [user.code, targetCode], (err) => {
                if (err) return callback({ success: false, message: '既に申請済みかフレンドです。' });
                callback({ success: true, message: '申請を送信しました。' });
                if (socketMap[targetCode]) io.to(socketMap[targetCode]).emit('friends_data_update');
            });
        });
    });

    socket.on('respond_friend_request', (data) => {
        const user = onlineUsers[socket.id];
        const { targetCode, accept } = data;
        if (!user) return;
        if (accept) {
            db.run(`UPDATE friends SET status = 'accepted' WHERE user_code = ? AND friend_code = ?`, [targetCode, user.code]);
            db.run(`INSERT OR IGNORE INTO friends (user_code, friend_code, status) VALUES (?, ?, 'accepted')`, [user.code, targetCode]);
        } else {
            db.run(`DELETE FROM friends WHERE user_code = ? AND friend_code = ?`, [targetCode, user.code]);
        }
        socket.emit('friends_data_update');
        if (socketMap[targetCode]) io.to(socketMap[targetCode]).emit('friends_data_update');
    });

    // 4. マッチメイキングと対戦
    socket.on('challenge_friend', (targetCode) => {
        const user = onlineUsers[socket.id];
        const targetSocketId = socketMap[targetCode];
        if (user && targetSocketId && onlineUsers[targetSocketId].status === 'idle') {
            io.to(targetSocketId).emit('incoming_challenge', { code: user.code, name: user.name });
        }
    });

    socket.on('respond_challenge', (data) => {
        const user = onlineUsers[socket.id];
        const { targetCode, accept } = data;
        const targetSocketId = socketMap[targetCode];
        if (accept && targetSocketId) {
            const matchId = `match_${Date.now()}`;
            activeMatches[matchId] = { p1: targetCode, p2: user.code, p1Ready: false, p2Ready: false };
            onlineUsers[socket.id].status = 'playing';
            onlineUsers[targetSocketId].status = 'playing';
            io.emit('friends_data_update'); 
            io.to(targetSocketId).emit('match_found', { matchId, opponentName: user.name, opponentCode: user.code });
            socket.emit('match_found', { matchId, opponentName: onlineUsers[targetSocketId].name, opponentCode: targetCode });
        } else if (targetSocketId) {
            io.to(targetSocketId).emit('challenge_rejected', { name: user.name });
        }
    });

    socket.on('join_random_match', () => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        user.status = 'matching';
        io.emit('friends_data_update');

        const validQueue = matchmakingQueue.filter(id => onlineUsers[id] && onlineUsers[id].status === 'matching');
        if (validQueue.length > 0) {
            const opponentSocketId = validQueue.shift();
            const opponent = onlineUsers[opponentSocketId];
            if (opponent && opponentSocketId !== socket.id) {
                const matchId = `match_${Date.now()}`;
                activeMatches[matchId] = { p1: opponent.code, p2: user.code, p1Ready: false, p2Ready: false };
                user.status = 'playing'; opponent.status = 'playing';
                matchmakingQueue = matchmakingQueue.filter(id => id !== opponentSocketId && id !== socket.id);
                io.emit('friends_data_update');
                
                io.to(opponentSocketId).emit('match_found', { matchId, opponentName: user.name, opponentCode: user.code });
                socket.emit('match_found', { matchId, opponentName: opponent.name, opponentCode: opponent.code });
                return;
            }
        }
        if (!matchmakingQueue.includes(socket.id)) matchmakingQueue.push(socket.id);
    });

    socket.on('match_ready', (data) => {
        const { matchId, gameType } = data;
        const match = activeMatches[matchId];
        if (!match) return;

        const user = onlineUsers[socket.id];
        if (match.p1 === user.code) { match.p1Ready = true; match.p1Type = gameType; }
        if (match.p2 === user.code) { match.p2Ready = true; match.p2Type = gameType; }

        if (match.p1Ready && match.p2Ready) {
            // DBにマッチ情報を初期登録 (winner_codeはNULL) - startedAtを記録しておく
            match.startedAt = Date.now();
            db.run(`INSERT INTO match_history (p1_code, p2_code, p1_type, p2_type, match_key) VALUES (?, ?, ?, ?, ?)`, 
                   [match.p1, match.p2, match.p1Type, match.p2Type, matchId], function(err) {
                if(!err) {
                    match.dbId = this.lastID;
                } else {
                    console.error('match_history INSERT error:', err);
                }
            });

            io.to(socketMap[match.p1]).emit('start_countdown', { opponentType: match.p2Type, opponentName: onlineUsers[socketMap[match.p2]].name });
            io.to(socketMap[match.p2]).emit('start_countdown', { opponentType: match.p1Type, opponentName: onlineUsers[socketMap[match.p1]].name });
        }
    });

    socket.on('game_action', (data) => {
        const { matchId, action, payload, gameType } = data;
        const match = activeMatches[matchId];
        if (!match) return;
        
        const user = onlineUsers[socket.id];
        const targetCode = (match.p1 === user.code) ? match.p2 : match.p1;
        const targetSocketId = socketMap[targetCode];
        
        if (targetSocketId) {
            io.to(targetSocketId).emit('opponent_action', { 
                action, 
                payload, 
                gameType 
            });
        }
    });

    // 5. ゲームオーバーと勝敗記録、ステータスリセット
    // クライアントがgame_overを送ってきた＝クライアントが負け
    socket.on('game_over', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        
        const matchId = data.matchId;
        const match = activeMatches[matchId];
        if (!match) return;

        const loserCode = user.code;
        const winnerCode = (match.p1 === loserCode) ? match.p2 : match.p1;
        recordMatchResult(matchId, match, winnerCode, loserCode, 'opponent_game_over', 'game_over');
    });
    
    socket.on('return_to_lobby', () => {
        const user = onlineUsers[socket.id];
        if (user) {
            user.status = 'idle';
            matchmakingQueue = matchmakingQueue.filter(id => id !== socket.id);
            
            // 進行中のマッチがあれば、対戦相手を勝者として記録・通知
            for (const matchId in activeMatches) {
                const match = activeMatches[matchId];
                if (match.p1 === user.code || match.p2 === user.code) {
                    if (match.gameOverHandled) { delete activeMatches[matchId]; continue; }
                    const winnerCode = (match.p1 === user.code) ? match.p2 : match.p1;
                    recordMatchResult(matchId, match, winnerCode, user.code, 'opponent_left', 'left_match');
                }
            }
            
            io.emit('friends_data_update');
        }
    });

// ----- ここから追加（io.on('connection', ...) の中） -----
    socket.on('get_chat_contacts', () => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        db.all(`SELECT u.code, u.name FROM friends f JOIN users u ON f.friend_code = u.code WHERE f.user_code = ? AND f.status = 'accepted'`, [user.code], (err, friends) => {
            db.all(`SELECT g.id, g.name, g.owner_code FROM chat_groups g JOIN group_members gm ON g.id = gm.group_id WHERE gm.user_code = ?`, [user.code], (err, groups) => {
                socket.emit('chat_contacts_data', { friends: friends || [], groups: groups || [] });
            });
        });
    });

    socket.on('create_group', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        const { name, members } = data; 
        db.run(`INSERT INTO chat_groups (name, owner_code) VALUES (?, ?)`, [name, user.code], function(err) {
            if (err) return;
            const groupId = this.lastID;
            const allMembers = [user.code, ...members];
            const stmt = db.prepare(`INSERT INTO group_members (group_id, user_code) VALUES (?, ?)`);
            allMembers.forEach(m => stmt.run(groupId, m));
            stmt.finalize();
            allMembers.forEach(m => {
                if (socketMap[m]) io.to(socketMap[m]).emit('chat_contacts_update');
            });
        });
    });

    socket.on('delete_group', (groupId) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        db.get(`SELECT owner_code FROM chat_groups WHERE id = ?`, [groupId], (err, row) => {
            if (row && row.owner_code === user.code) {
                db.all(`SELECT user_code FROM group_members WHERE group_id = ?`, [groupId], (err, members) => {
                    db.run(`DELETE FROM chat_groups WHERE id = ?`, [groupId]);
                    db.run(`DELETE FROM group_members WHERE group_id = ?`, [groupId]);
                    if(members) {
                        members.forEach(m => {
                            if (socketMap[m.user_code]) io.to(socketMap[m.user_code]).emit('chat_contacts_update');
                        });
                    }
                });
            }
        });
    });

    socket.on('send_chat_message', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        const { targetCode, groupId, message } = data;
        db.run(`INSERT INTO messages (sender_code, receiver_code, group_id, message) VALUES (?, ?, ?, ?)`,
            [user.code, targetCode || null, groupId || null, message], function(err) {
            if (err) return;
            const msgObj = { id: this.lastID, sender_code: user.code, sender_name: user.name, receiver_code: targetCode, group_id: groupId, message, timestamp: new Date() };
            
            if (groupId) {
                db.all(`SELECT user_code FROM group_members WHERE group_id = ?`, [groupId], (err, members) => {
                    if (members) {
                        members.forEach(m => {
                            if (socketMap[m.user_code]) io.to(socketMap[m.user_code]).emit('receive_chat_message', msgObj);
                        });
                    }
                });
            } else if (targetCode) {
                socket.emit('receive_chat_message', msgObj);
                if (socketMap[targetCode]) io.to(socketMap[targetCode]).emit('receive_chat_message', msgObj);
            }
        });
    });

    socket.on('get_chat_messages', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        const { targetCode, groupId } = data;
        if (groupId) {
            db.all(`SELECT m.*, u.name as sender_name FROM messages m JOIN users u ON m.sender_code = u.code WHERE m.group_id = ? ORDER BY m.timestamp ASC`, [groupId], (err, rows) => {
                socket.emit('chat_messages_data', rows || []);
            });
        } else if (targetCode) {
            db.all(`SELECT m.*, u.name as sender_name FROM messages m JOIN users u ON m.sender_code = u.code WHERE (m.sender_code = ? AND m.receiver_code = ?) OR (m.sender_code = ? AND m.receiver_code = ?) ORDER BY m.timestamp ASC`, 
            [user.code, targetCode, targetCode, user.code], (err, rows) => {
                socket.emit('chat_messages_data', rows || []);
            });
        }
    });

    socket.on('vc_signal', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        if (socketMap[data.target]) {
            io.to(socketMap[data.target]).emit('vc_signal', { sender: user.code, signal: data.signal });
        }
    });

// --------------------------------------------------
    // グループボイスチャットのシグナリング
    socket.on('group_vc_signal', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        const { groupId, signal, target } = data;
        
        if (target) {
            // 特定のターゲット（アンサーを返す相手など）が指定されている場合
            if (socketMap[target]) {
                io.to(socketMap[target]).emit('group_vc_signal', { sender: user.code, groupId, signal });
            }
        } else {
            // ターゲット指定がない場合は、自分以外のグループメンバー全員にブロードキャスト
            db.all(`SELECT user_code FROM group_members WHERE group_id = ?`, [groupId], (err, members) => {
                if (members) {
                    members.forEach(m => {
                        if (m.user_code !== user.code && socketMap[m.user_code]) {
                            io.to(socketMap[m.user_code]).emit('group_vc_signal', { sender: user.code, groupId, signal });
                        }
                    });
                }
            });
        }
    });

    // グループメンバーの追加（オーナーのみ）
    socket.on('add_group_member', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        const { groupId, memberCode } = data;
        
        db.get(`SELECT owner_code FROM chat_groups WHERE id = ?`, [groupId], (err, row) => {
            if (row && row.owner_code === user.code) {
                db.run(`INSERT OR IGNORE INTO group_members (group_id, user_code) VALUES (?, ?)`, [groupId, memberCode], () => {
                    if (socketMap[memberCode]) io.to(socketMap[memberCode]).emit('chat_contacts_update');
                    io.to(socket.id).emit('chat_contacts_update');
                });
            }
        });
    });

    // グループメンバーの削除（オーナーのみ）
    socket.on('remove_group_member', (data) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        const { groupId, memberCode } = data;
        
        db.get(`SELECT owner_code FROM chat_groups WHERE id = ?`, [groupId], (err, row) => {
            if (row && row.owner_code === user.code && memberCode !== user.code) { // オーナー自身は削除できないようにする
                db.run(`DELETE FROM group_members WHERE group_id = ? AND user_code = ?`, [groupId, memberCode], () => {
                    if (socketMap[memberCode]) io.to(socketMap[memberCode]).emit('chat_contacts_update');
                    io.to(socket.id).emit('chat_contacts_update');
                });
            }
        });
    });

    // グループからの脱退（オーナー以外）
    socket.on('leave_group', (groupId) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        
        db.get(`SELECT owner_code FROM chat_groups WHERE id = ?`, [groupId], (err, row) => {
            if (row && row.owner_code !== user.code) { // オーナーは脱退できない（削除機能を使う）
                db.run(`DELETE FROM group_members WHERE group_id = ? AND user_code = ?`, [groupId, user.code], () => {
                    socket.emit('chat_contacts_update');
                });
            }
        });
    });

    // メッセージの削除機能（自分の送信したメッセージのみ削除可能）
    socket.on('delete_message', (messageId) => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        
        db.get(`SELECT sender_code, group_id, receiver_code FROM messages WHERE id = ?`, [messageId], (err, row) => {
            if (row && row.sender_code === user.code) {
                db.run(`DELETE FROM messages WHERE id = ?`, [messageId], () => {
                    // グループメッセージの場合、全メンバーに削除を通知
                    if (row.group_id) {
                        db.all(`SELECT user_code FROM group_members WHERE group_id = ?`, [row.group_id], (err, members) => {
                            if (members) {
                                members.forEach(m => { 
                                    if (socketMap[m.user_code]) {
                                        io.to(socketMap[m.user_code]).emit('message_deleted', messageId); 
                                    }
                                });
                            }
                        });
                    } 
                    // 個人メッセージの場合、お互いに削除を通知
                    else if (row.receiver_code) {
                        socket.emit('message_deleted', messageId);
                        if (socketMap[row.receiver_code]) {
                            io.to(socketMap[row.receiver_code]).emit('message_deleted', messageId);
                        }
                    }
                });
            }
        });
    });

    // アカウントの削除機能
    socket.on('delete_account', () => {
        const user = onlineUsers[socket.id];
        if (!user) return;
        const code = user.code;
        
        // 関連するユーザーデータを削除
        db.run(`DELETE FROM users WHERE code = ?`, [code]);
        db.run(`DELETE FROM friends WHERE user_code = ? OR friend_code = ?`, [code, code]);
        db.run(`DELETE FROM group_members WHERE user_code = ?`, [code]);
        db.run(`DELETE FROM messages WHERE sender_code = ? OR receiver_code = ?`, [code, code]);
        // ※オーナーになっているグループ自体の削除が必要な場合は、連鎖削除の処理を追加してください。
        
        delete socketMap[code];
        delete onlineUsers[socket.id];
        
        io.emit('friends_data_update');
        socket.emit('account_deleted');
        socket.disconnect();
    });
    // --------------------------------------------------
    // ----- ここまで追加 -----

    socket.on('disconnect', () => {
        const user = onlineUsers[socket.id];
        if (user) {
            const disconnectedCode = user.code;
            matchmakingQueue = matchmakingQueue.filter(id => id !== socket.id);
            
            for (const matchId in activeMatches) {
                const match = activeMatches[matchId];
                if (match.p1 === disconnectedCode || match.p2 === disconnectedCode) {
                    if (match.gameOverHandled) { delete activeMatches[matchId]; continue; }

                    const opponentCode = (match.p1 === disconnectedCode) ? match.p2 : match.p1;
                    const opponentSocketId = socketMap[opponentCode];
                    if (opponentSocketId) {
                        io.to(opponentSocketId).emit('opponent_connection_wait', { graceMs: RECONNECT_GRACE_MS });
                    }

                    // すぐに敗北扱いにはせず、一定時間だけ再接続を待つ。
                    // 通信が一瞬途切れただけで敗北扱いになってしまうのを防ぐため。
                    setTimeout(() => {
                        const stillActive = activeMatches[matchId];
                        if (!stillActive || stillActive.gameOverHandled) return;

                        if (socketMap[disconnectedCode]) {
                            // 猶予時間内に再ログイン（再接続）できていたので、試合を続行する
                            if (opponentSocketId) io.to(opponentSocketId).emit('opponent_reconnected');
                            return;
                        }

                        // 猶予時間内に再接続がなければ、正式に切断負けとして確定する
                        recordMatchResult(matchId, stillActive, opponentCode, disconnectedCode, 'disconnect', 'disconnect');
                    }, RECONNECT_GRACE_MS);
                }
            }

            delete socketMap[disconnectedCode];
            delete onlineUsers[socket.id];
            io.emit('friends_data_update');
            io.emit('online_count', Object.keys(socketMap).length);
        }
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
