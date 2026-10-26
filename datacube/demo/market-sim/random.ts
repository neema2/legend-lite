// JAVA'S RANDOM, exactly (java.util.Random: its 48-bit linear congruential generator, Knuth's), so the market in the tab
// is the market the Deephaven server makes (market.groovy, `new Random(seed)`): the same seed, the same draws, the same
// world -- names, prices, stories. Its 48-bit state is two 24-bit halves, each product exact in a double's 53 bits.

const TWO_24 = 0x1000000;
/** The multiplier 0x5DEECE66D, in 24-bit halves. */
const M_HI = 0x5de;
const M_LO = 0xece66d;

export class JavaRandom {
  #hi = 0;
  #lo = 0;
  #nextGaussian: number | null = null;

  /** Seeded as `new java.util.Random(seed)`: the seed scrambled with the multiplier, its low 48 bits kept. */
  constructor(seed: number | bigint) {
    const scrambled = (BigInt(seed) ^ 0x5deece66dn) & ((1n << 48n) - 1n);
    this.#hi = Number(scrambled >> 24n);
    this.#lo = Number(scrambled & 0xffffffn);
  }

  /** The generator's next `bits` bits (at most 32), as Java's `next`: a signed 32-bit int for 32. */
  next(bits: number): number {
    const lo = this.#lo * M_LO + 0xb;
    const carry = Math.floor(lo / TWO_24);
    const hi = (this.#hi * M_LO + this.#lo * M_HI + carry) % TWO_24;
    this.#lo = lo % TWO_24;
    this.#hi = hi;
    // the state's top `bits` bits: (hi * 2^24 + lo) >>> (48 - bits)
    const value = Math.floor((this.#hi * TWO_24 + this.#lo) / 2 ** (48 - bits));
    return value | 0;
  }

  nextInt(bound?: number): number {
    if (bound === undefined) return this.next(32);
    if (!(bound > 0)) throw new RangeError('bound must be positive');
    // a power of two: the high bits (a power of two's product is exact in a double)
    if ((bound & -bound) === bound) return Math.floor((bound * this.next(31)) / 0x80000000);
    let bits: number;
    let val: number;
    do {
      bits = this.next(31);
      val = bits % bound;
    } while (bits - val + (bound - 1) > 0x7fffffff);
    return val;
  }

  nextDouble(): number {
    return (this.next(26) * 0x8000000 + this.next(27)) / 2 ** 53;
  }

  nextBoolean(): boolean {
    return this.next(1) !== 0;
  }

  /** Java's polar method (Marsaglia's), a pair a time: the second kept for the next call. */
  nextGaussian(): number {
    if (this.#nextGaussian !== null) {
      const g = this.#nextGaussian;
      this.#nextGaussian = null;
      return g;
    }
    let v1: number;
    let v2: number;
    let s: number;
    do {
      v1 = 2 * this.nextDouble() - 1;
      v2 = 2 * this.nextDouble() - 1;
      s = v1 * v1 + v2 * v2;
    } while (s >= 1 || s === 0);
    const multiplier = Math.sqrt(-2 * Math.log(s) / s);
    this.#nextGaussian = v2 * multiplier;
    return v1 * multiplier;
  }

  /** `xs` shuffled in place as Java's Collections.shuffle(list, random) does it. */
  shuffle<T>(xs: T[]): T[] {
    for (let i = xs.length; i > 1; i--) {
      const j = this.nextInt(i);
      const t = xs[i - 1]!;
      xs[i - 1] = xs[j]!;
      xs[j] = t;
    }
    return xs;
  }
}
