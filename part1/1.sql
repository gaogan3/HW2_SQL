{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "original-calcium",
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
   "execution_count": 7,
   "id": "similar-meditation",
   "metadata": {},
   "outputs": [],
   "source": [
    "with open('/home/jovyan/shared/__DEMO/miba_postgresql.json') as file:\n",
    "    access_data = json.load(file)\n",
    "    \n",
    "    \n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "imposed-injury",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "              diff_sum\n",
      "0  35.3232342007434944\n"
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
    "        cur.execute(\"\"\" SELECT AVG(a.sum1 - b.sum2) AS diff_sum\n",
    "                        FROM (SELECT COUNT(DISTINCT seat_no) AS sum1,aircraft_code\n",
    "                                FROM seats\n",
    "                                GROUP BY aircraft_code) AS a,\n",
    "                             (SELECT COUNT(DISTINCT seat_no) AS sum2, flight_id\n",
    "                                FROM boarding_passes\n",
    "                                GROUP BY flight_id) AS b,flights c\n",
    "                        WHERE a.aircraft_code = c.aircraft_code\n",
    "                        AND b.flight_id = c.flight_id;       \n",
    "        \"\"\")\n",
    "        print(DF(cur.fetchall()))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "organic-finland",
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
