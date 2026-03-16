"""
GPU Resource Monitor
=====================
Monitors NVIDIA GPU availability, memory, and utilization before
heavy processing operations. Provides safe multiprocessing limits
based on available GPU resources.

Public API:
    get_gpu_status()         → dict with memory/utilization info
    check_gpu_available()    → bool
    get_safe_worker_count()  → int (max parallel workers given GPU memory)
    log_gpu_status()         → None (logs current GPU state)
"""

import logging
import subprocess
import shutil

logger = logging.getLogger(__name__)

# Minimum free GPU memory (MB) required before starting a heavy operation
_MIN_FREE_MB = 512

# Estimated GPU memory per concurrent heavy task (MB)
_MB_PER_TASK = 1024


def get_gpu_status() -> dict:
    """
    Query nvidia-smi for GPU memory and utilization.

    Returns:
        {
            "available": bool,
            "gpu_name": str,
            "total_memory_mb": int,
            "used_memory_mb": int,
            "free_memory_mb": int,
            "utilization_pct": int,
            "error": str | None,
        }
    """
    result = {
        "available": False,
        "gpu_name": "",
        "total_memory_mb": 0,
        "used_memory_mb": 0,
        "free_memory_mb": 0,
        "utilization_pct": 0,
        "error": None,
    }

    if not shutil.which("nvidia-smi"):
        result["error"] = "nvidia-smi not found"
        return result

    try:
        proc = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=name,memory.total,memory.used,memory.free,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if proc.returncode != 0:
            result["error"] = f"nvidia-smi failed: {proc.stderr.strip()}"
            return result

        line = proc.stdout.strip().split("\n")[0]
        parts = [p.strip() for p in line.split(",")]

        if len(parts) >= 5:
            result["available"] = True
            result["gpu_name"] = parts[0]
            result["total_memory_mb"] = int(parts[1])
            result["used_memory_mb"] = int(parts[2])
            result["free_memory_mb"] = int(parts[3])
            result["utilization_pct"] = int(parts[4])

    except subprocess.TimeoutExpired:
        result["error"] = "nvidia-smi timed out"
    except (ValueError, IndexError) as exc:
        result["error"] = f"Failed to parse nvidia-smi output: {exc}"
    except Exception as exc:
        result["error"] = f"GPU monitoring error: {exc}"

    return result


def check_gpu_available(min_free_mb: int = _MIN_FREE_MB) -> bool:
    """Return True if GPU has at least *min_free_mb* MB free."""
    status = get_gpu_status()
    if not status["available"]:
        return False
    return status["free_memory_mb"] >= min_free_mb


def get_safe_worker_count(
    max_workers: int = 4,
    mb_per_task: int = _MB_PER_TASK,
) -> int:
    """
    Calculate the maximum number of parallel workers that can run
    safely given current GPU memory availability.

    Returns at least 1 (serial processing) and at most *max_workers*.
    If no GPU is detected, returns *max_workers* (CPU-only path).
    """
    status = get_gpu_status()

    if not status["available"]:
        # No GPU — use CPU workers, no GPU constraint
        return max_workers

    free_mb = status["free_memory_mb"]
    gpu_workers = max(1, free_mb // mb_per_task)

    safe = min(max_workers, gpu_workers)
    logger.info(
        "GPU worker limit: %d (free=%dMB, per_task=%dMB, max=%d)",
        safe, free_mb, mb_per_task, max_workers,
    )
    return safe


def log_gpu_status(label: str = "") -> None:
    """Log current GPU status at INFO level."""
    status = get_gpu_status()
    if not status["available"]:
        logger.info("GPU status [%s]: not available (%s)",
                    label, status.get("error", "unknown"))
        return

    logger.info(
        "GPU status [%s]: %s — %dMB/%dMB used (%dMB free), %d%% utilization",
        label,
        status["gpu_name"],
        status["used_memory_mb"],
        status["total_memory_mb"],
        status["free_memory_mb"],
        status["utilization_pct"],
    )


def release_gpu_memory(label: str = "") -> None:
    """Release cached GPU memory if PyTorch/CUDA is available.

    Calls torch.cuda.empty_cache() to free cached memory blocks back to
    the CUDA allocator, and runs gc.collect() first to ensure Python
    objects holding CUDA tensors are freed.

    Safe to call even when torch is not installed or no GPU is present.
    """
    import gc
    gc.collect()

    try:
        import torch
        if torch.cuda.is_available():
            torch.cuda.empty_cache()
            logger.info("GPU memory released (%s)", label)
    except ImportError:
        pass  # torch not installed — skip
    except Exception as exc:
        logger.debug("GPU memory release failed (%s): %s", label, exc)

