// @vitest-environment jsdom
import "@testing-library/jest-dom/vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import { ChatComposer } from "./ChatComposer";
afterEach(cleanup);
it("focuses a requested draft without submitting it", () => {
  const send = vi.fn();
  const props = {model:"smart" as const, setModel:vi.fn(), effort:"off" as const, setEffort:vi.fn(), style:"concise" as const, setStyle:vi.fn(), busy:false, onSend:send, onStop:vi.fn()};
  const {rerender} = render(<ChatComposer {...props}/>);
  rerender(<ChatComposer {...props} prefill={{text:"Tell me about this", request:1}}/>);
  expect(screen.getByRole("textbox", {name:"Question"})).toHaveFocus();
  expect(screen.getByRole("textbox", {name:"Question"})).toHaveValue("Tell me about this");
  expect(send).not.toHaveBeenCalled();
});
