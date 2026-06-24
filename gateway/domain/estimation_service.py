from typing import Optional
from gateway.shared.interfaces import StateRepository
from gateway.shared.models import EstimationState, TimeBucket


class EstimationService:
    """任务预估时间计算服务（领域层），使用双桶轮换算法。"""

    def __init__(self, state_repo: StateRepository, bucket_capacity: int = 100):
        self._state_repo = state_repo
        self._bucket_capacity = bucket_capacity
        loaded_state = state_repo.get_estimation_state()
        if loaded_state is None:
            # 初始化空状态
            self._state = EstimationState(
                active=TimeBucket(avg_ms=0, count=0),
                staging=TimeBucket(avg_ms=0, count=0),
            )
        else:
            self._state = loaded_state

    def calculate_estimation(self) -> Optional[int]:
        """计算预估时间（毫秒），返回 None 表示无足够数据。"""
        if self._state.active.count < 3:
            return None
        return self._state.active.avg_ms

    def record_completion(self, duration_ms: int) -> None:
        """记录任务完成时间，更新双桶状态。"""
        active = self._state.active
        staging = self._state.staging

        # 阶段由 active.count 推导：count < N 为初始阶段，count >= N 为轮换阶段
        if active.count < self._bucket_capacity:
            # 初始阶段：只更新active
            new_count = active.count + 1
            new_avg = (active.avg_ms * active.count + duration_ms) // new_count
            self._state.active = TimeBucket(avg_ms=new_avg, count=new_count)
        else:
            # 轮换阶段：同时更新active和staging
            # 更新active
            new_active_count = active.count + 1
            new_active_avg = (
                active.avg_ms * active.count + duration_ms
            ) // new_active_count
            self._state.active = TimeBucket(
                avg_ms=new_active_avg, count=new_active_count
            )

            # 更新staging
            new_staging_count = staging.count + 1
            new_staging_avg = (
                staging.avg_ms * staging.count + duration_ms
            ) // new_staging_count
            self._state.staging = TimeBucket(
                avg_ms=new_staging_avg, count=new_staging_count
            )

            # staging达到容量后，将staging复制到active并重置staging
            if new_staging_count >= self._bucket_capacity:
                self._state.active = TimeBucket(
                    avg_ms=new_staging_avg, count=new_staging_count
                )
                self._state.staging = TimeBucket(avg_ms=0, count=0)

        # 持久化状态
        self._state_repo.save_estimation_state(self._state)
