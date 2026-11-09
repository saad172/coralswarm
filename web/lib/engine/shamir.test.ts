import { describe, it, expect } from "vitest";
import { split, combine } from "./shamir";

function randomSecret(n = 32): Uint8Array {
  const b = new Uint8Array(n);
  globalThis.crypto.getRandomValues(b);
  return b;
}

describe("engine/shamir", () => {
  it("reconstructs from exactly threshold shares", () => {
    const secret = randomSecret();
    const shares = split(secret, 5, 3);
    expect([...combine([shares[0], shares[2], shares[4]])]).toEqual([...secret]);
  });

  it("reconstructs from a different threshold subset", () => {
    const secret = randomSecret();
    const shares = split(secret, 5, 3);
    expect([...combine([shares[1], shares[2], shares[3]])]).toEqual([...secret]);
  });

  it("reconstructs from more than threshold shares", () => {
    const secret = randomSecret();
    const shares = split(secret, 4, 2);
    expect([...combine(shares)]).toEqual([...secret]);
  });

  it("fewer than threshold shares does NOT reveal the secret", () => {
    const secret = randomSecret();
    const shares = split(secret, 5, 3);
    expect([...combine([shares[0], shares[1]])]).not.toEqual([...secret]);
  });

  it("rejects invalid parameters", () => {
    expect(() => split(randomSecret(), 3, 5)).toThrow();
    expect(() => split(randomSecret(), 2, 1)).toThrow();
  });

  it("rejects duplicate share points on combine", () => {
    const shares = split(randomSecret(), 5, 3);
    expect(() => combine([shares[0], shares[0], shares[1]])).toThrow();
  });
});
