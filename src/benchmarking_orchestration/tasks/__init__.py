import logging
from exorcist.taskdb import _logger
from exorcist.models import TaskStatus
from typing import Iterable
import exorcist
import sqlalchemy as sqla


class TaskStatusDB(exorcist.TaskStatusDB):
    @staticmethod
    def _create_empty_db(metadata, engine):
        sqla.Table(
            "task_types",
            metadata,
            sqla.Column("taskid", sqla.String, sqla.ForeignKey("tasks.taskid")),
            sqla.Column("task_type", sqla.String),
        )
        return exorcist.TaskStatusDB._create_empty_db(metadata, engine)

    @property
    def task_types_table(self):
        return self.metadata.tables["task_types"]

    def add_task_type(self, taskid, task_type):
        with self.engine.begin() as conn:
            conn.execute(
                sqla.insert(self.task_types_table).values(
                    [
                        {"taskid": taskid, "task_type": task_type},
                    ]
                )
            )

    def add_task_with_type(
        self,
        taskid: str,
        requirements: Iterable[str],
        max_tries: int,
        task_type: str,
    ):
        super().add_task(taskid, requirements, max_tries)
        self.add_task_type(taskid, task_type)

    def check_out_task_with_type(self, task_type: str):
        _logger.info("Checking out task")
        subq = (
            sqla.select(self.tasks_table.c.taskid)
            .where(self.tasks_table.c.status == TaskStatus.AVAILABLE.value)
            .where(self.task_types_table.c.task_type == task_type)
            .limit(1)
            .scalar_subquery()
        )

        with self.engine.begin() as conn:
            update_stmt = self._task_row_update_statement(
                taskid=subq,
                status=TaskStatus.IN_PROGRESS,
                is_checkout=True,
                old_status=TaskStatus.AVAILABLE,
            ).returning(self.tasks_table.c.taskid)
            result = list(conn.execute(update_stmt))

        if len(result) == 1:
            taskid = result[0][0]
        elif len(result) == 0:
            _logger.info("Unable to select an available task")
            return None  # skip extra logging
        else:  # -no-cov-
            raise RuntimeError(
                f"Received {len(result)} task IDs to check "
                "out. Something went very weird."
            )

        # log the changed row if we're doing DEBUG logging
        if _logger.isEnabledFor(logging.DEBUG):
            reselect = sqla.select(self.tasks_table).where(
                self.tasks_table.c.taskid == taskid
            )
            # read-only; use connect() (no autocommit)
            with self.engine.connect() as conn:
                reloaded = list(conn.execute(reselect).all())

            assert len(reloaded) == 1, f"Got {len(reloaded)} rows for '{taskid}'"

            claimed = reloaded[0]
            _logger.debug(f"After claiming task: {claimed=}")

        _logger.info(f"Selected task '{taskid}'")
        return taskid
