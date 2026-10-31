import type { SimEvent } from '../domain/events';

export class SimPriorityQueue {
  private heap: SimEvent[] = [];

  public push(event: SimEvent): void {
    this.heap.push(event);
    this.bubbleUp(this.heap.length - 1);
  }

  public pop(): SimEvent | undefined {
    if (this.heap.length === 0) return undefined;
    const top = this.heap[0];
    const bottom = this.heap.pop()!;
    if (this.heap.length > 0) {
      this.heap[0] = bottom;
      this.sinkDown(0);
    }
    return top;
  }

  public peek(): SimEvent | undefined {
    return this.heap[0];
  }

  public size(): number {
    return this.heap.length;
  }

  public clear(): void {
    this.heap = [];
  }

  public toArray(): SimEvent[] {
    return [...this.heap].sort((a, b) => this.compare(a, b));
  }

  private compare(a: SimEvent, b: SimEvent): number {
    if (a.scheduledTime !== b.scheduledTime) {
      return a.scheduledTime - b.scheduledTime;
    }
    return a.seq - b.seq; // Monotonic tie-breaker
  }

  private bubbleUp(index: number): void {
    let curr = index;
    while (curr > 0) {
      const parent = Math.floor((curr - 1) / 2);
      if (this.compare(this.heap[curr], this.heap[parent]) < 0) {
        const temp = this.heap[curr];
        this.heap[curr] = this.heap[parent];
        this.heap[parent] = temp;
        curr = parent;
      } else {
        break;
      }
    }
  }

  private sinkDown(index: number): void {
    let curr = index;
    const length = this.heap.length;

    while (true) {
      let smallest = curr;
      const left = 2 * curr + 1;
      const right = 2 * curr + 2;

      if (left < length && this.compare(this.heap[left], this.heap[smallest]) < 0) {
        smallest = left;
      }
      if (right < length && this.compare(this.heap[right], this.heap[smallest]) < 0) {
        smallest = right;
      }

      if (smallest !== curr) {
        const temp = this.heap[curr];
        this.heap[curr] = this.heap[smallest];
        this.heap[smallest] = temp;
        curr = smallest;
      } else {
        break;
      }
    }
  }
}
