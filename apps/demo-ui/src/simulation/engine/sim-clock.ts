export type SimClockStatus = 'STOPPED' | 'RUNNING' | 'PAUSED';

export class SimClock {
  public virtualTimeMs: number = 0;
  public speedMultiplier: number = 1.0;
  public status: SimClockStatus = 'STOPPED';

  public start(): void {
    this.status = 'RUNNING';
  }

  public pause(): void {
    this.status = 'PAUSED';
  }

  public resume(): void {
    if (this.status === 'PAUSED' || this.status === 'STOPPED') {
      this.status = 'RUNNING';
    }
  }

  public reset(): void {
    this.virtualTimeMs = 0;
    this.status = 'STOPPED';
  }

  public setSpeed(multiplier: number): void {
    if (!Number.isFinite(multiplier)) return;
    this.speedMultiplier = Math.max(0.1, Math.min(10.0, multiplier));
  }

  public advance(realDeltaMs: number): number {
    if (this.status !== 'RUNNING') return this.virtualTimeMs;
    if (!Number.isFinite(realDeltaMs) || realDeltaMs < 0) return this.virtualTimeMs;
    const virtualDelta = realDeltaMs * this.speedMultiplier;
    this.virtualTimeMs += virtualDelta;
    return this.virtualTimeMs;
  }

  public jumpTo(targetVirtualMs: number): void {
    if (targetVirtualMs >= this.virtualTimeMs) {
      this.virtualTimeMs = targetVirtualMs;
    }
  }
}
