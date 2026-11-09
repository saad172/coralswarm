/** PostCSS config so Next.js runs Tailwind. Without this, the @tailwind
 *  directives in globals.css emit no utility CSS and the app renders unstyled. */
export default {
  plugins: {
    tailwindcss: {},
  },
};
