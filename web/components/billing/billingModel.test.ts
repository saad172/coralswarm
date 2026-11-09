import { describe, expect, it } from "vitest";
import { paymentMethodLine } from "./billingModel";

describe("paymentMethodLine", () => {
  it("is null when nothing is on file", () => {
    expect(paymentMethodLine(null)).toBeNull();
    expect(paymentMethodLine(undefined)).toBeNull();
  });

  it("reads a card with its brand, last four and expiry", () => {
    expect(paymentMethodLine({ type: "card", brand: "visa", last4: "4242", exp_month: 4, exp_year: 2028 })).toEqual({
      label: "Visa ending 4242",
      detail: "expires 04/2028",
    });
  });

  it("reads a card from the older shape that carried no expiry", () => {
    expect(paymentMethodLine({ type: "card", brand: "amex", last4: "0005" })).toEqual({
      label: "Amex ending 0005",
      detail: null,
    });
  });

  it("reads a US bank account as the bank name and last four", () => {
    expect(paymentMethodLine({ type: "us_bank_account", bank_name: "Example Bank", last4: "4242" })).toEqual({
      label: "Example Bank •••• 4242",
      detail: null,
    });
    expect(paymentMethodLine({ type: "us_bank_account", last4: "4242" })?.label).toBe("Bank account •••• 4242");
  });

  it("reads Link as Link, with the email when Stripe exposes one", () => {
    expect(paymentMethodLine({ type: "link", email: "a@example.com" })).toEqual({ label: "Link", detail: "a@example.com" });
    expect(paymentMethodLine({ type: "link" })).toEqual({ label: "Link", detail: null });
  });

  it("names any other type plainly", () => {
    expect(paymentMethodLine({ type: "sepa_debit", last4: "3000" })?.label).toBe("Sepa debit •••• 3000");
  });
});
