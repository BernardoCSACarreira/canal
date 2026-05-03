export interface CanalBuffer<T> {
  enqueue(items: readonly T[]): Promise<void>;
  dequeueBatch(max: number): Promise<T[]>;
}

export class InMemoryCanalBuffer<T> implements CanalBuffer<T> {
  private readonly q: T[] = [];

  async enqueue(items: readonly T[]): Promise<void> {
    this.q.push(...items);
  }

  async dequeueBatch(max: number): Promise<T[]> {
    const n = Math.min(max, this.q.length);
    return this.q.splice(0, n);
  }
}
