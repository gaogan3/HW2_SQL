{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "naval-multimedia",
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
   "id": "tribal-matrix",
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
   "execution_count": null,
   "id": "billion-encyclopedia",
   "metadata": {},
   "outputs": [],
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
    "                SELECT a.passenger_name\n",
    "                FROM tickets a\n",
    "                WHERE a.ticket_no = (SELECT b.ticket_no\n",
    "                                      FROM ticket_flights b,\n",
    "                                    (SELECT h.flight_id,max(h.actual_departure - h.scheduled_departure + h.actual_arrival - h.scheduled_departure)\n",
    "                                      FROM flights h) d\n",
    "                                     WHERE b.flight_id = d.flight_id\n",
    "                                     AND (a.ticket_no LIKE '%B%'\n",
    "                                     OR a.ticket_no LIKE '%E%')\n",
    "                                     );\n",
    "        \"\"\")\n",
    "        print(DF(cur.fetchall()))"
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
