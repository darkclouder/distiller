import os
import sqlite3
import json
import sys
import datetime

from distiller.core.Logger import Logger
from ..interfaces.Meta import Meta
from distiller.utils.TaskLoader import TaskLoader, TaskLoadError
from distiller.utils.PathFinder import PathFinder
from distiller.core.impl.SimpleScheduler.SchedulingInfo import SchedulingInfo
from distiller.api.AbstractTask import parameter_id, spirit_id_to_label


class SQLiteMeta(Meta):
    def __init__(self, env):
        self.env = env
        self.logger = self.env.logger.claim("Core")

        self.__try_create_db()

        with self.__connect_db():
            self.logger.notice("Meta database loaded")

    def __connect_db(self):
        data_root = PathFinder.get_data_root()
        db_path = os.path.join(data_root, self.env.config.get("meta.file_path"))

        try:
            if not os.path.exists(data_root):
                os.makedirs(data_root)

            db = sqlite3.connect(
                db_path,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            )
 
            return db
        except Exception as e:
            self.logger.critical(e)
            return

    def __try_create_db(self):
        if self.env.config.get("meta.volatile"):
            db_path = os.path.join(PathFinder.get_data_root(), self.env.config.get("meta.file_path"))
            if os.path.exists(db_path):
                os.remove(db_path)

        with self.__connect_db() as conn:
            queries = [
                """
                CREATE TABLE IF NOT EXISTS Casks (
                    spirit_id VARCHAR PRIMARY KEY,
                    last_completion DATETIME
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS ScheduledTargets (
                    schedule_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    spirit_name VARCHAR,
                    parameters VARCHAR,
                    execution_start DATETIME,
                    execution_end DATETIME,
                    age_requirement INTEGER,
                    priority INTEGER NOT NULL
                )
                """
            ]

            for query in queries:
                conn.execute(query)
            conn.commit()

    def get_cask(self, spirit):
        with self.__connect_db() as conn:
            spirit_id = spirit.label()

            csr = conn.execute(
                'SELECT last_completion AS "[timestamp]" FROM Casks WHERE spirit_id=?',
                (spirit_id,)
            )
            row = csr.fetchone()

            if row is None:
                return None

            return {"last_completion": row[0]}

    def update_cask(self, spirit, completion=None):
        if completion is None:
            completion = datetime.datetime.now()

        with self.__connect_db() as conn:
            spirit_id = spirit.label()

            csr = conn.execute("SELECT 1 FROM Casks WHERE spirit_id=?", (spirit_id,))
            row = csr.fetchone()

            if row is None:
                conn.execute(
                    "INSERT INTO Casks (spirit_id, last_completion) VALUES (?,?)",
                    (spirit_id, completion)
                )
            else:
                conn.execute("UPDATE Casks SET last_completion=? WHERE spirit_id=?", (completion, spirit_id))

    def get_scheduled_spirits(self):
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
