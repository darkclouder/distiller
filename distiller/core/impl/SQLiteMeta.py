import os
import sqlite3
import json
import datetime

from ..interfaces.Meta import Meta
from distiller.core.impl.SimpleScheduler.SchedulingInfo import SchedulingInfo
from distiller.api.AbstractTask import parameter_id, spirit_id_to_label


class SQLiteMeta(Meta):
    def __init__(self, env):
        self.env = env
        self.logger = self.env.logger.claim("Core")

        self.db_path = self.env.config.get("meta.file_path", path=True)

        self.__try_create_db()

        with self.__connect_db():
            self.logger.notice("Meta database loaded")

    def __connect_db(self):
        data_root = os.path.dirname(self.db_path)

        try:
            if not os.path.exists(data_root):
                os.makedirs(data_root)

            db = sqlite3.connect(
                self.db_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
 
            return db
        except Exception as e:
            self.logger.critical(e)
            return

    def __try_create_db(self):
        # Is database set to volatile?
        # Volatile: with each restart the meta data is reset
        if self.env.config.get("meta.volatile"):
            if os.path.exists(self.db_path):
                os.remove(self.db_path)

        with self.__connect_db() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS Casks (
                    spirit_name VARCHAR NOT NULL,
                    parameters VARCHAR NOT NULL,
                    last_completion DATETIME
                )
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS spirit_id
                ON Casks(spirit_name, parameters)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ScheduledTargets (
                    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    spirit_name VARCHAR,
                    parameters VARCHAR,
                    execution_start DATETIME,
                    execution_end DATETIME,
                    age_requirement INTEGER,
                    priority INTEGER NOT NULL
                )
            """)
            conn.commit()

    def get_cask(self, spirit_id):
        with self.__connect_db() as conn:
            spirit_name, parameters = spirit_id
            enc_parameters = json.dumps(parameters)

            csr = conn.execute(
                'SELECT last_completion AS "[timestamp]" FROM Casks WHERE spirit_name=? AND parameters=?',
                (spirit_name, enc_parameters)
            )
            row = csr.fetchone()

            if row is None:
                return None

            return {
                "spirit_id": (spirit_name, parameters),
                "last_completion": row[2]
            }

    def get_all_casks(self):
        with self.__connect_db() as conn:
            csr = conn.execute(
                'SELECT spirit_name, parameters, last_completion AS "[timestamp]" FROM Casks'
            )

            return [
                {
                    "spirit_id": (row[0], json.loads(row[1])),
                    "last_completion": row[2]
                }
                for row in csr.fetchall()
            ]

    def update_cask(self, spirit_id, completion=None):
        if completion is None:
            completion = datetime.datetime.now()

        with self.__connect_db() as conn:
            spirit_name, parameters = spirit_id
            enc_parameters = json.dumps(parameters)

            csr = conn.execute(
                "SELECT 1 FROM Casks WHERE spirit_name=? AND parameters=?",
                (spirit_name, enc_parameters)
            )
            row = csr.fetchone()

            if row is None:
                conn.execute(
                    "INSERT INTO Casks (spirit_name, parameters, last_completion) VALUES (?,?,?)",
                    (spirit_name, enc_parameters, completion)
                )
            else:
                conn.execute(
                    "UPDATE Casks SET last_completion=? WHERE spirit_name=? AND parameters=?",
                    (completion, spirit_name, enc_parameters)
                )

    def invalidate_cask(self, spirit_id):
        with self.logger.catch(sqlite3.OperationalError).critical():
            with self.__connect_db() as conn:
                spirit_name, parameters = spirit_id
                enc_parameters = json.dumps(parameters)

                csr = conn.execute(
                    "DELETE FROM Casks WHERE spirit_name=? AND parameters=?",
                    (spirit_name, enc_parameters)
                )

        if csr.rowcount < 1:
            raise ValueError("Cask for spirit %i does not exist" % spirit_id)

    def get_scheduled_infos(self):
        with self.logger.catch(sqlite3.Error).critical():
            with self.__connect_db() as conn:
                csr = conn.execute("""
                    SELECT
                        schedule_id,
                        spirit_name,
                        parameters,
                        execution_start  AS "[timestamp] start",
                        execution_end  AS "[timestamp] end",
                        age_requirement,
                        priority
                    FROM ScheduledTargets
                    WHERE execution_end is NULL OR execution_end >= datetime('now')
                """)

            return [
                spi_sched
                for spi_sched in (self.__schedule_to_spirit(row) for row in csr)
                if spi_sched is not None
            ]

        return []

    def __schedule_to_spirit(self, schedule_row):
        parameters = json.loads(schedule_row[2])

        return SchedulingInfo(
            (schedule_row[1], parameters),
            schedule_row[5],
            None,
            priority=schedule_row[6],
            reoccurring=True,
            start_date=schedule_row[3],
            end_date=schedule_row[4],
            schedule_id=schedule_row[0]
        )

    def remove_scheduled_spirit(self, spirit_id):
        with self.logger.catch(sqlite3.OperationalError).critical():
            with self.__connect_db() as conn:
                csr = conn.execute(
                    "DELETE FROM ScheduledTargets WHERE spirit_name=? AND parameters=?",
                    spirit_id_to_label(*spirit_id)
                )

        if csr.rowcount < 1:
            raise ValueError("Schedule spirit %i does not exist" % spirit_id)

    def add_scheduled_spirit(self, schedule_info):
        with self.logger.catch(sqlite3.OperationalError).critical():
            with self.__connect_db() as conn:
                csr = conn.execute("""
                    INSERT INTO ScheduledTargets
                    (spirit_name, parameters, execution_start, execution_end, age_requirement, priority)
                    VALUES (?,?,?,?,?,?)
                """, (
                    schedule_info.spirit_id[0],
                    parameter_id(schedule_info.spirit_id[1]),
                    schedule_info.start_date,
                    schedule_info.end_date,
                    schedule_info.age_requirement,
                    schedule_info.priority
                ))

            schedule_info.schedule_id = csr.lastrowid

            return schedule_info

        return None


module_class = SQLiteMeta
