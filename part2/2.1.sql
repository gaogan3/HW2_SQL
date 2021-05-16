{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 68,
   "id": "million-florida",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Requirement already satisfied: psycopg2-binary in /opt/conda/lib/python3.8/site-packages (2.8.6)\n",
      "Note: you may need to restart the kernel to use updated packages.\n"
     ]
    }
   ],
   "source": [
    "pip install psycopg2-binary\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 111,
   "id": "powerful-party",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pandas import DataFrame as df"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "closing-culture",
   "metadata": {},
   "outputs": [],
   "source": [
    "import json\n",
    "import psycopg2\n",
    "from psycopg2.extras import RealDictCursor"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "looking-customer",
   "metadata": {},
   "source": [
    "Part 2:\n",
    "You can use SQLite for this task. But PostgreSQL is preferred (you need to run your own database server on your computer to do that). The solution for this part is CREATE query + INSERT query which launch result should be a table structure with following requirements filled with a piece of data. Slash between two datatypes means OR, e.g. TEXT / UUID means TEXT or UUID.\n",
    "\n",
    "1. Bring life back to music\n",
    "Let's describe some music label history.\n",
    "\n",
    "Band\n",
    "\n",
    "band_id (TEXT / UUID) — primary key\n",
    "name (TEXT)\n",
    "genre (TEXT)\n",
    "Album\n",
    "\n",
    "album_id (TEXT / UUID) — primary key\n",
    "band_id - references Band.band_id\n",
    "name (TEXT)\n",
    "date_released (DATE / INTEGER)\n",
    "Song\n",
    "\n",
    "album_id — references Album.album_id\n",
    "song_id (TEXT / UUID) — primary key\n",
    "name (TEXT)\n",
    "duration (INTEGER)\n",
    "lyrics_author — references Musician.musician_id\n",
    "music_author — references Musician.musician_id\n",
    "Musician\n",
    "\n",
    "musician_id (TEXT / UUID) — primary key\n",
    "name (TEXT)\n",
    "musician_band\n",
    "\n",
    "musician_id — references Musician.musician_id\n",
    "band_id — references Band.band_id\n",
    "started_at (DATE / INTEGER)\n",
    "finished_at (DATE / INTEGER)\n",
    "instrument (TEXT)\n",
    "Restrictions should be following:\n",
    "\n",
    "If anyone deletes any band, all the corresponding data should be deleted with cascade method.\n",
    "No one can delete any musician while there is at least one corresponding record exists in any other table.\n",
    "Choose any music band you like and fill the tables with at least couple rows each table."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 27,
   "id": "hazardous-nirvana",
   "metadata": {},
   "outputs": [],
   "source": [
    "import sqlite3 \n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 34,
   "id": "aware-preservation",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "[]\n"
     ]
    }
   ],
   "source": [
    "conn = sqlite3.connect(':memory:')\n",
    "cur = conn.cursor()\n",
    "cur.execute('''\n",
    "CREATE TABLE band(\n",
    "    band_id TEXT  PRIMARY KEY,\n",
    "    name TEXT NOT NULL,\n",
    "    genre TEXT NOT NULL)\n",
    "    ''')\n",
    "print(cur.fetchall())"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 48,
   "id": "clinical-revelation",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 48,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "CREATE TABLE album(\n",
    "    album_id TEXT  PRIMARY KEY,\n",
    "    band_id TEXT NOT NULL,\n",
    "    name TEXT NOT NULL,\n",
    "    date_relased DATE NOT NULL,\n",
    "    Foreign KEY (band_id) references band(band_id)\n",
    "    )\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 49,
   "id": "impressed-opinion",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 49,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "CREATE TABLE musician(\n",
    "    musician_id TEXT  PRIMARY KEY,\n",
    "    name TEXT NOT NULL\n",
    "    )\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 55,
   "id": "derived-identification",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 55,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "CREATE TABLE song(\n",
    "    song  TEXT PRIMARY KEY,\n",
    "    name TEXT NOT NULL,\n",
    "    duration INTEGER NOT NULL, \n",
    "    album_id TEXT NOT NULL,\n",
    "    lyrics_author TEXT NOT NULL,\n",
    "    music_author TEXT NOT NULL,\n",
    "    Foreign KEY (album_id) references album(album_id),\n",
    "    Foreign KEY (lyrics_author) references musician(musician_id),\n",
    "    Foreign KEY (music_author) references musician(musician_id)\n",
    "    )\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 64,
   "id": "numerical-documentary",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 64,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "CREATE TABLE musician_band(\n",
    "    musician_id TEXT NOT NULL,\n",
    "    band_id TEXT NOT NULL,\n",
    "    started_at DATE NOT NULL, \n",
    "    finished_at DATET NOT NULL,\n",
    "    instrument TEXT NOT NULL,\n",
    "    music_author TEXT NOT NULL,\n",
    "    Foreign KEY (musician_id) references musician(musician_id)\n",
    "    )\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 103,
   "id": "simple-general",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 103,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "CREATE TABLE musician_band1(\n",
    "    musician_id TEXT NOT NULL,\n",
    "    band_id TEXT NOT NULL,\n",
    "    started_at DATE NOT NULL, \n",
    "    finished_at DATE NOT NULL,\n",
    "    instrument TEXT NOT NULL,\n",
    "    music_author TEXT NOT NULL,\n",
    "    \n",
    "    constraint musian_id_fk\n",
    "    Foreign KEY (musician_id) references musician(musician_id) on delete restrict,\n",
    "    constraint band_id_fk\n",
    "    Foreign KEY (band_id) references band(band_id) on delete cascade\n",
    "    )\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 104,
   "id": "plain-press",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 104,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "drop table musician_band\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 105,
   "id": "chronic-thong",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 105,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "alter table musician_band1 rename to musician_band\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 95,
   "id": "owned-parks",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 95,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "CREATE TABLE album1(\n",
    "    album_id TEXT  PRIMARY KEY,\n",
    "    band_id TEXT NOT NULL,\n",
    "    name TEXT NOT NULL,\n",
    "    date_relased DATE NOT NULL,\n",
    "    constraint band_id_fk \n",
    "    Foreign KEY (band_id) references band(band_id) on delete cascade\n",
    "    )\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 97,
   "id": "paperback-universal",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 97,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "alter table album1 rename to album\n",
    "    ''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 112,
   "id": "forced-toilet",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "   0              1     2\n",
      "0  1          QUEEN  rock\n",
      "1  2  one direction   pop\n"
     ]
    }
   ],
   "source": [
    "cur.execute('SELECT * from band')\n",
    "print(df(cur.fetchall()))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 39,
   "id": "radio-forest",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 39,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "INSERT INTO band VALUES (1,'QUEEN','rock')\n",
    "''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 106,
   "id": "fancy-singer",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 106,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "INSERT INTO band VALUES (2,'one direction','pop')\n",
    "''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 113,
   "id": "forbidden-indonesia",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 113,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "INSERT INTO album VALUES (1,1,'news of the world',1977),(2,2,'four',2014)\n",
    "''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 115,
   "id": "persistent-fighter",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 115,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "INSERT INTO song VALUES (1,1,'We Are the Champions',208,1,1),(2,2,'Ready to Run',196,2,2)\n",
    "''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 118,
   "id": "expressed-wilson",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 118,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "INSERT INTO musician VALUES (1,'Freddie Mercury'),(3,'Brian May'),(4,'Roger Taylor'),(5,'John Deacon'),(2,'Niall Horan'),(6,'Liam Payne'),(7,'Harry Styles'),(8,'Louis Tomlinson')\n",
    "''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 126,
   "id": "alternate-confidence",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "<sqlite3.Cursor at 0x7f18c2716b20>"
      ]
     },
     "execution_count": 126,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute('''\n",
    "INSERT INTO musician_band VALUES (1,1,1970,1997,'piano',0),(3,1,1970,1997,'guitar',0),(4,1,1970,1997,'drum',0),(5,1,1970,1997,'bass',0),(2,2,2010,0,'guitar',0),(6,2,2010,0,'singer',0),(7,2,2010,0,'singer',0),(8,2,2010,0,'singer',0)\n",
    "''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 127,
   "id": "boolean-discretion",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "   0  1     2     3       4  5\n",
      "0  1  1  1970  1997   piano  0\n",
      "1  3  1  1970  1997  guitar  0\n",
      "2  4  1  1970  1997    drum  0\n",
      "3  5  1  1970  1997    bass  0\n",
      "4  2  2  2010     0  guitar  0\n",
      "5  6  2  2010     0  singer  0\n",
      "6  7  2  2010     0  singer  0\n",
      "7  8  2  2010     0  singer  0\n"
     ]
    }
   ],
   "source": [
    "cur.execute(''' SELECT * from musician_band\n",
    "''')\n",
    "print(df(cur.fetchall()))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 139,
   "id": "studied-roads",
   "metadata": {},
   "outputs": [
    {
     "ename": "OperationalError",
     "evalue": "near \"header\": syntax error",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mOperationalError\u001b[0m                          Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-139-b8d1c6367b26>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m\u001b[0m\n\u001b[0;32m----> 1\u001b[0;31m \u001b[0mcur\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mexecute\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m'''header on'''\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m",
      "\u001b[0;31mOperationalError\u001b[0m: near \"header\": syntax error"
     ]
    }
   ],
   "source": [
    "cur.execute('''header on''')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 153,
   "id": "entertaining-cincinnati",
   "metadata": {},
   "outputs": [],
   "source": [
    "def dict_factory(cursor, row):\n",
    "    d = {}\n",
    "    for idx, col in enumerate(cursor.description):\n",
    "        d[col[0]] = row[idx]\n",
    "    return d"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 163,
   "id": "promising-plaza",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "  band_id  name genre album_id  date_relased  song duration lyrics_author  \\\n",
      "0    None  None  rock     None          1977  None     None          None   \n",
      "1    None  None   pop     None          2014  None     None          None   \n",
      "\n",
      "  music_author musician_id started_at finished_at instrument  \n",
      "0         None        None       None        None       None  \n",
      "1         None        None       None        None       None  \n"
     ]
    }
   ],
   "source": [
    "cur.row_factory = dict_factory\n",
    "cur.execute(\"\"\"\n",
    "        SELECT *\n",
    "        FROM band \n",
    "       inner JOIN album ON album.band_id = band.band_id\n",
    "       left JOIN song ON song.album_id = album.album_id\n",
    "       left JOIN musician ON song.music_author = musician.musician_id\n",
    "       left JOIN musician_band ON musician_band.musician_id = musician.musician_id\n",
    "    \"\"\")\n",
    "print(df(cur.fetchall()))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 156,
   "id": "twelve-criticism",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[{'cid': 0,\n",
       "  'name': 'musician_id',\n",
       "  'type': 'TEXT',\n",
       "  'notnull': 1,\n",
       "  'dflt_value': None,\n",
       "  'pk': 0},\n",
       " {'cid': 1,\n",
       "  'name': 'band_id',\n",
       "  'type': 'TEXT',\n",
       "  'notnull': 1,\n",
       "  'dflt_value': None,\n",
       "  'pk': 0},\n",
       " {'cid': 2,\n",
       "  'name': 'started_at',\n",
       "  'type': 'DATE',\n",
       "  'notnull': 1,\n",
       "  'dflt_value': None,\n",
       "  'pk': 0},\n",
       " {'cid': 3,\n",
       "  'name': 'finished_at',\n",
       "  'type': 'DATET',\n",
       "  'notnull': 1,\n",
       "  'dflt_value': None,\n",
       "  'pk': 0},\n",
       " {'cid': 4,\n",
       "  'name': 'instrument',\n",
       "  'type': 'TEXT',\n",
       "  'notnull': 1,\n",
       "  'dflt_value': None,\n",
       "  'pk': 0},\n",
       " {'cid': 5,\n",
       "  'name': 'music_author',\n",
       "  'type': 'TEXT',\n",
       "  'notnull': 1,\n",
       "  'dflt_value': None,\n",
       "  'pk': 0}]"
      ]
     },
     "execution_count": 156,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute(''' PRAGMA table_info(musician_band)\n",
    "''')\n",
    "cur.fetchall()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 148,
   "id": "billion-forestry",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[('album',), ('band',), ('musician',), ('musician_band',), ('song',)]"
      ]
     },
     "execution_count": 148,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    " cur.execute(''' select name from sqlite_master where type='table' order by name;\n",
    "''')\n",
    "cur.fetchall()\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 145,
   "id": "welcome-showcase",
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/plain": [
       "[('table',\n",
       "  'band',\n",
       "  'band',\n",
       "  2,\n",
       "  'CREATE TABLE band(\\n    band_id TEXT  PRIMARY KEY,\\n    name TEXT NOT NULL,\\n    genre TEXT NOT NULL)'),\n",
       " ('index', 'sqlite_autoindex_band_1', 'band', 3, None),\n",
       " ('table',\n",
       "  'musician',\n",
       "  'musician',\n",
       "  6,\n",
       "  'CREATE TABLE musician(\\n    musician_id TEXT  PRIMARY KEY,\\n    name TEXT NOT NULL\\n    )'),\n",
       " ('index', 'sqlite_autoindex_musician_1', 'musician', 7, None),\n",
       " ('table',\n",
       "  'song',\n",
       "  'song',\n",
       "  8,\n",
       "  'CREATE TABLE song(\\n    song  TEXT PRIMARY KEY,\\n    name TEXT NOT NULL,\\n    duration INTEGER NOT NULL, \\n    album_id TEXT NOT NULL,\\n    lyrics_author TEXT NOT NULL,\\n    music_author TEXT NOT NULL,\\n    Foreign KEY (album_id) references album(album_id),\\n    Foreign KEY (lyrics_author) references musician(musician_id),\\n    Foreign KEY (music_author) references musician(musician_id)\\n    )'),\n",
       " ('index', 'sqlite_autoindex_song_1', 'song', 9, None),\n",
       " ('table',\n",
       "  'album',\n",
       "  'album',\n",
       "  11,\n",
       "  'CREATE TABLE \"album\"(\\n    album_id TEXT  PRIMARY KEY,\\n    band_id TEXT NOT NULL,\\n    name TEXT NOT NULL,\\n    date_relased DATE NOT NULL,\\n    constraint band_id_fk \\n    Foreign KEY (band_id) references band(band_id) on delete cascade\\n    )'),\n",
       " ('index', 'sqlite_autoindex_album_1', 'album', 12, None),\n",
       " ('table',\n",
       "  'musician_band',\n",
       "  'musician_band',\n",
       "  4,\n",
       "  'CREATE TABLE \"musician_band\"(\\n    musician_id TEXT NOT NULL,\\n    band_id TEXT NOT NULL,\\n    started_at DATE NOT NULL, \\n    finished_at DATET NOT NULL,\\n    instrument TEXT NOT NULL,\\n    music_author TEXT NOT NULL,\\n    \\n    constraint musian_id_fk\\n    Foreign KEY (musician_id) references musician(musician_id) on delete restrict,\\n    constraint band_id_fk\\n    Foreign KEY (band_id) references band(band_id) on delete cascade\\n    )')]"
      ]
     },
     "execution_count": 145,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "cur.execute(''' select * from sqlite_master\n",
    "''')\n",
    "cur.fetchall()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.6"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
