"""In-memory state for the SABnzbd mock."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

PRIORITY_ORDER: dict[str, int] = {"Force": 0, "High": 1, "Normal": 2, "Low": 3}
PRIORITY_INT_TO_STR: dict[int, str] = {2: "Force", 1: "High", 0: "Normal", -1: "Low"}
PRIORITY_STR_TO_INT: dict[str, int] = {v: k for k, v in PRIORITY_INT_TO_STR.items()}


@dataclass
class MockJob:
    nzo_id: str
    filename: str
    cat: str
    status: str = "Downloading"
    priority: str = "Normal"
    index: int = 0
    mb: str = "700.0"
    mbleft: str = "700.0"
    percentage: str = "0"
    timeleft: str = "0:10:00"
    labels: list[str] = field(default_factory=list)
    paused: bool = False


class SABnzbdState:
    def __init__(self) -> None:
        self.jobs: dict[str, MockJob] = {}
        self.queue_paused: bool = False
        self._counter: int = 0

    def reset(self) -> None:
        self.jobs.clear()
        self.queue_paused = False
        self._counter = 0

    def generate_nzo_id(self) -> str:
        self._counter += 1
        return f"SABnzbd_nzo_{self._counter:06d}"

    def add_job(
        self,
        filename: str,
        cat: str = "*",
        priority: str = "Normal",
        nzo_id: str | None = None,
    ) -> str:
        if nzo_id is None:
            nzo_id = self.generate_nzo_id()
        job = MockJob(
            nzo_id=nzo_id,
            filename=filename,
            cat=cat,
            priority=priority,
            index=len(self.jobs),
        )
        self.jobs[nzo_id] = job
        self._reindex()
        return nzo_id

    def remove_job(self, nzo_id: str) -> None:
        self.jobs.pop(nzo_id, None)
        self._reindex()

    def _reindex(self) -> None:
        sorted_jobs = sorted(
            self.jobs.values(),
            key=lambda j: (PRIORITY_ORDER.get(j.priority, 2), j.index),
        )
        for i, job in enumerate(sorted_jobs):
            job.index = i

    # -- queue-level operations --

    def pause_queue(self) -> dict[str, Any]:
        self.queue_paused = True
        return {"status": True}

    def resume_queue(self) -> dict[str, Any]:
        self.queue_paused = False
        return {"status": True}

    # -- job-level operations --

    def pause_job(self, nzo_id: str) -> dict[str, Any]:
        if nzo_id in self.jobs:
            self.jobs[nzo_id].paused = True
        return {"status": True, "nzo_ids": [nzo_id]}

    def resume_job(self, nzo_id: str) -> dict[str, Any]:
        if nzo_id in self.jobs:
            self.jobs[nzo_id].paused = False
        return {"status": True, "nzo_ids": [nzo_id]}

    def delete_job(self, nzo_id: str) -> dict[str, Any]:
        self.jobs.pop(nzo_id, None)
        self._reindex()
        return {"status": True, "nzo_ids": [nzo_id]}

    def set_priority(self, nzo_id: str, priority_int: int) -> dict[str, Any]:
        if nzo_id in self.jobs:
            self.jobs[nzo_id].priority = PRIORITY_INT_TO_STR.get(priority_int, "Normal")
            self._reindex()
        position = self.jobs[nzo_id].index if nzo_id in self.jobs else 0
        return {"position": position}

    def switch_jobs(self, nzo_id: str, other_nzo_id: str) -> dict[str, Any]:
        if nzo_id not in self.jobs or other_nzo_id not in self.jobs:
            return {"result": [0, 0]}
        job = self.jobs[nzo_id]
        other = self.jobs[other_nzo_id]
        sorted_jobs = sorted(self.jobs.values(), key=lambda j: j.index)
        sorted_jobs.remove(job)
        target_idx = sorted_jobs.index(other)
        sorted_jobs.insert(target_idx, job)
        for i, j in enumerate(sorted_jobs):
            j.index = i
        prio_int = PRIORITY_STR_TO_INT.get(job.priority, 0)
        return {"result": [job.index, prio_int]}

    # -- read operations --

    def get_queue_response(self) -> dict[str, Any]:
        slots = sorted(self.jobs.values(), key=lambda j: j.index)
        if self.queue_paused:
            status = "Paused"
        elif slots:
            status = "Downloading"
        else:
            status = "Idle"
        return {
            "queue": {
                "status": status,
                "paused": self.queue_paused,
                "noofslots": len(slots),
                "noofslots_total": len(slots),
                "slots": [
                    {
                        "nzo_id": j.nzo_id,
                        "filename": j.filename,
                        "cat": j.cat,
                        "status": "Paused" if j.paused else j.status,
                        "priority": j.priority,
                        "index": j.index,
                        "mb": j.mb,
                        "mbleft": j.mbleft,
                        "percentage": j.percentage,
                        "timeleft": j.timeleft,
                        "labels": j.labels,
                    }
                    for j in slots
                ],
            }
        }

    def get_history_response(self) -> dict[str, Any]:
        return {
            "history": {
                "slots": [],
                "noofslots": 0,
                "total_size": "0 B",
                "month_size": "0 B",
                "week_size": "0 B",
                "day_size": "0 B",
            }
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "jobs": {
                nzo_id: {
                    "nzo_id": j.nzo_id,
                    "filename": j.filename,
                    "cat": j.cat,
                    "status": j.status,
                    "priority": j.priority,
                    "index": j.index,
                    "paused": j.paused,
                }
                for nzo_id, j in self.jobs.items()
            },
            "queue_paused": self.queue_paused,
            "counter": self._counter,
        }
