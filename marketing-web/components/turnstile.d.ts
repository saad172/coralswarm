/* Minimal ambient type for the Turnstile script loaded in
   SectionContact. Only the calls this site makes are declared; the
   widget itself renders implicitly from the .cf-turnstile div, so
   render()/getResponse() are not needed. */

interface TurnstileApi {
  /** Re-arm the widget. A token is single-use, so a failed submit
      must reset before the visitor can try again. */
  reset: (widget?: string | HTMLElement) => void;
}

interface Window {
  turnstile?: TurnstileApi;
}
