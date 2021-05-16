{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "bearing-wealth",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pandas import DataFrame as DF\n",
    "import json\n",
    "import psycopg2\n",
    "from psycopg2.extras import RealDictCursor"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "facial-organic",
   "metadata": {},
   "outputs": [],
   "source": [
    "with open('/home/jovyan/shared/__DEMO/miba_postgresql.json') as file:\n",
    "    access_data = json.load(file)\n",
    "    \n",
    "    "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "worldwide-professional",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "  aircraft_code                     bt\n",
      "0           319 0 days 05:41:36.508022\n",
      "1           763 0 days 04:05:45.990973\n",
      "2           733 0 days 02:23:10.601179\n"
     ]
    }
   ],
   "source": [
    "with psycopg2.connect(\n",
    "    dbname='demo', \n",
    "    user=access_data['user'],\n",
    "    password=access_data['pass'], \n",
    "    host='10.0.0.28',\n",
    "    cursor_factory=RealDictCursor\n",
    ") as conn:\n",
    "     with conn.cursor() as cur:\n",
    "        cur.execute(\"\"\"\n",
    "                SELECT a.aircraft_code, avg(a.actual_arrival - a.actual_departure) AS bt\n",
    "                FROM flights a\n",
    "                GROUP BY a.aircraft_code\n",
    "                ORDER BY bt DESC\n",
    "                LIMIT 3;\n",
    "        \"\"\")\n",
    "        print(DF(cur.fetchall()))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "polish-reaction",
   "metadata": {},
   "outputs": [],
   "source": []
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
