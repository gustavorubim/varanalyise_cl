"""Run context holding per-run mutable state with thread safety.

Each analysis run gets its own RunContext, eliminating module-level mutable
globals and enabling concurrent runs in the same process.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from va_agent.models import Finding, ReportSection
from va_agent.sql.executor import SQLExecutor


@dataclass
class RunContext:
    """Per-run state container for executor, findings, and report sections.

    Thread-safe: all mutations go through the lock.
    """

    executor: SQLExecutor
    _findings: list[Finding] = field(default_factory=list)
    _sections: list[ReportSection] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add_finding(self, finding: Finding) -> int:
        """Append a finding and return the new count (thread-safe)."""
        with self._lock:
            self._findings.append(finding)
            return len(self._findings)

    def add_section(self, section: ReportSection) -> int:
        """Append a section and return the new count (thread-safe)."""
        with self._lock:
            self._sections.append(section)
            return len(self._sections)

    @property
    def findings(self) -> list[Finding]:
        with self._lock:
            return list(self._findings)

    @property
    def sections(self) -> list[ReportSection]:
        with self._lock:
            return list(self._sections)

    @property
    def finding_count(self) -> int:
        with self._lock:
            return len(self._findings)

    def reset(self) -> None:
        """Clear findings and sections (for reuse between runs)."""
        with self._lock:
            self._findings.clear()
            self._sections.clear()
