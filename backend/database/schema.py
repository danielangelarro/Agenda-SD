import sqlite3

def setup_database(db_name='agenda.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Tabla de usuarios
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL
        );
    ''')

    # Tabla de grupos
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT,
            is_hierarchical BOOLEAN NOT NULL,
            creator_id INTEGER,
            FOREIGN KEY (creator_id) REFERENCES users(id)
        );
    ''')

    # Tabla de eventos
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            description TEXT,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            creator_id INTEGER NOT NULL,
            group_id INTEGER,
            is_group_event BOOLEAN NOT NULL DEFAULT 0,
            is_hierarchical_event BOOLEAN NOT NULL DEFAULT 0,
            FOREIGN KEY (creator_id) REFERENCES users(id),
            FOREIGN KEY (group_id) REFERENCES groups(id)
        );
    ''')

    # Tabla de miembros de grupo
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_groups (
            user_id INTEGER,
            group_id INTEGER,
            is_leader BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, group_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (group_id) REFERENCES groups(id)
        );
    ''')

    # Tabla de participantes de eventos
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS event_participants (
            event_id INTEGER,
            user_id INTEGER,
            is_accepted BOOLEAN NOT NULL DEFAULT 0,
            PRIMARY KEY (event_id, user_id),
            FOREIGN KEY (event_id) REFERENCES events(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    ''')

    # Tabla de invitaciones de grupo
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS group_invitations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            invited_user_id INTEGER NOT NULL,
            inviter_user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            FOREIGN KEY (group_id) REFERENCES groups(id),
            FOREIGN KEY (invited_user_id) REFERENCES users(id),
            FOREIGN KEY (inviter_user_id) REFERENCES users(id)
        );
    ''')

    # Conflictos (útil para eventos jerárquicos que se imponen aunque haya solapamiento)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS event_conflicts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            reason TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (event_id) REFERENCES events(id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    ''')

    cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_conflicts_user ON event_conflicts(user_id);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_event_conflicts_event ON event_conflicts(event_id);')

    # Migraciones livianas: agregar columnas faltantes sin romper DBs existentes.
    cursor.execute("PRAGMA table_info(events);")
    event_cols = {row[1] for row in cursor.fetchall()}  # row[1] = name
    if "is_hierarchical_event" not in event_cols:
        cursor.execute("ALTER TABLE events ADD COLUMN is_hierarchical_event BOOLEAN NOT NULL DEFAULT 0;")

    conn.commit()
    conn.close()
