/* @ds-bundle: {"format":3,"namespace":"CoralSwarmDesignSystem_91cd0b","components":[{"name":"Badge","sourcePath":"components/Badge/Badge.jsx"},{"name":"Button","sourcePath":"components/Button/Button.jsx"},{"name":"Card","sourcePath":"components/Card/Card.jsx"},{"name":"CiteChip","sourcePath":"components/CiteChip/CiteChip.jsx"},{"name":"Display","sourcePath":"components/Display/Display.jsx"},{"name":"Eyebrow","sourcePath":"components/Eyebrow/Eyebrow.jsx"},{"name":"HueChip","sourcePath":"components/HueChip/HueChip.jsx"},{"name":"Input","sourcePath":"components/Input/Input.jsx"},{"name":"Nav","sourcePath":"components/Nav/Nav.jsx"},{"name":"Pill","sourcePath":"components/Pill/Pill.jsx"},{"name":"Section","sourcePath":"components/Section/Section.jsx"},{"name":"DotMandala","sourcePath":"components/Wordmark/Wordmark.jsx"},{"name":"Wordmark","sourcePath":"components/Wordmark/Wordmark.jsx"}],"sourceHashes":{"components/Badge/Badge.jsx":"5a08e291be53","components/Button/Button.jsx":"3ca254aaf42c","components/Card/Card.jsx":"21d0388d23e8","components/CiteChip/CiteChip.jsx":"29749686d8c0","components/Display/Display.jsx":"0d0b0e3ddf3d","components/Eyebrow/Eyebrow.jsx":"c89181508b3b","components/HueChip/HueChip.jsx":"92d901d33007","components/Input/Input.jsx":"dd355926576f","components/Nav/Nav.jsx":"c328f26b4077","components/Pill/Pill.jsx":"12256c8d1adc","components/Section/Section.jsx":"a9b0749deb87","components/Wordmark/Wordmark.jsx":"2e031956cfb5"},"inlinedExternals":[],"unexposedExports":[]} */

(() => {

const __ds_ns = (window.CoralSwarmDesignSystem_91cd0b = window.CoralSwarmDesignSystem_91cd0b || {});

const __ds_scope = {};

(__ds_ns.__errors = __ds_ns.__errors || []);

// components/Badge/Badge.jsx
try { (() => {
const React = window.React;
const HUES = {
  coral: "var(--hue-coral)",
  pink: "var(--hue-pink)",
  amber: "var(--hue-amber)",
  teal: "var(--hue-teal)",
  plum: "var(--hue-plum)"
};

/**
 * CoralSwarm Badge — kind/status label.
 * variant "solid" fills with the hue; "outline" is mono, uppercase, hue-bordered.
 */
function Badge({
  hue = "coral",
  variant = "solid",
  children,
  style,
  ...rest
}) {
  const color = HUES[hue] || hue;
  const base = {
    display: "inline-flex",
    alignItems: "center",
    gap: 6,
    borderRadius: "var(--radius-pill)",
    fontWeight: 500,
    lineHeight: 1,
    ...style
  };
  const variants = {
    solid: {
      padding: "4px 10px",
      fontSize: 12,
      background: color,
      color: "#fff"
    },
    outline: {
      padding: "3px 10px",
      fontSize: 11,
      fontFamily: "var(--mono)",
      letterSpacing: "0.06em",
      textTransform: "uppercase",
      background: "transparent",
      border: `1px solid ${color}`,
      color
    }
  };
  return React.createElement("span", {
    style: {
      ...base,
      ...variants[variant]
    },
    ...rest
  }, children);
}
Object.assign(__ds_scope, { Badge });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Badge/Badge.jsx", error: String((e && e.message) || e) }); }

// components/Button/Button.jsx
try { (() => {
const React = window.React;

/**
 * CoralSwarm Button — pill, three weights, three sizes.
 * Renders <a> when href is set, otherwise <button>.
 */
function Button({
  variant = "primary",
  size = "md",
  href,
  onClick,
  disabled = false,
  type = "button",
  children,
  style,
  ...rest
}) {
  const sizes = {
    sm: {
      padding: "6px 14px",
      fontSize: 13
    },
    md: {
      padding: "10px 18px",
      fontSize: 14
    },
    lg: {
      padding: "14px 24px",
      fontSize: 15
    }
  };
  const variants = {
    primary: {
      background: "var(--brand)",
      color: "#fff",
      border: "1px solid transparent"
    },
    ink: {
      background: "var(--ink)",
      color: "var(--bg)",
      border: "1px solid transparent"
    },
    secondary: {
      background: "var(--bg)",
      color: "var(--ink)",
      border: "1px solid var(--line)"
    },
    ghost: {
      background: "transparent",
      color: "var(--ink-2)",
      border: "1px solid transparent"
    },
    danger: {
      background: "transparent",
      color: "var(--bad)",
      border: "1px solid var(--bad)"
    }
  };
  const base = {
    fontFamily: "var(--sans)",
    fontWeight: 500,
    borderRadius: "var(--radius-pill)",
    display: "inline-flex",
    alignItems: "center",
    gap: 8,
    textDecoration: "none",
    cursor: disabled ? "not-allowed" : "pointer",
    opacity: disabled ? 0.5 : 1,
    lineHeight: 1,
    whiteSpace: "nowrap",
    transition: "transform var(--dur) var(--ease), box-shadow var(--dur) var(--ease), background var(--dur) var(--ease)",
    ...sizes[size],
    ...variants[variant],
    ...style
  };
  const [hover, setHover] = React.useState(false);
  const lifts = variant === "primary" || variant === "ink";
  const hoverStyle = hover && !disabled ? {
    transform: "translateY(-1px)",
    boxShadow: lifts ? "var(--shadow-md)" : "none"
  } : null;
  const props = {
    style: {
      ...base,
      ...hoverStyle
    },
    onMouseEnter: () => setHover(true),
    onMouseLeave: () => setHover(false),
    onClick: disabled ? undefined : onClick,
    ...rest
  };
  if (href && !disabled) {
    return React.createElement("a", {
      href,
      ...props
    }, children);
  }
  return React.createElement("button", {
    type,
    disabled,
    ...props
  }, children);
}
Object.assign(__ds_scope, { Button });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Button/Button.jsx", error: String((e && e.message) || e) }); }

// components/Card/Card.jsx
try { (() => {
const React = window.React;
const HUES = {
  coral: "var(--hue-coral)",
  pink: "var(--hue-pink)",
  amber: "var(--hue-amber)",
  teal: "var(--hue-teal)",
  plum: "var(--hue-plum)"
};

/**
 * CoralSwarm reef Card — kind shown by a 3px hue left-border + kind chip,
 * never by tinting the surface. Provenance line always visible.
 */
function Card({
  kind,
  hue = "coral",
  title,
  prov,
  children,
  style,
  ...rest
}) {
  const color = HUES[hue] || hue;
  const wrap = {
    background: "var(--bg)",
    border: "1px solid var(--line)",
    borderLeft: `3px solid ${color}`,
    borderRadius: "var(--radius-lg)",
    padding: 18,
    fontFamily: "var(--sans)",
    ...style
  };
  const kindChip = kind ? React.createElement("span", {
    style: {
      fontFamily: "var(--mono)",
      fontSize: 10,
      letterSpacing: "0.14em",
      textTransform: "uppercase",
      color: "var(--ink-3)",
      display: "inline-flex",
      alignItems: "center",
      gap: 6
    }
  }, React.createElement("span", {
    style: {
      width: 8,
      height: 8,
      borderRadius: "50%",
      background: color
    }
  }), kind) : null;
  const titleEl = title ? React.createElement("h3", {
    style: {
      fontFamily: "var(--display)",
      fontWeight: 400,
      fontSize: 22,
      margin: "10px 0 6px",
      color: "var(--ink)"
    }
  }, title) : null;
  const body = children ? React.createElement("p", {
    style: {
      fontSize: 14,
      color: "var(--ink-2)",
      margin: 0,
      lineHeight: 1.5
    }
  }, children) : null;
  const provEl = prov ? React.createElement("div", {
    style: {
      fontFamily: "var(--mono)",
      fontSize: 10,
      color: "var(--ink-3)",
      marginTop: 14,
      letterSpacing: "0.04em"
    }
  }, prov) : null;
  return React.createElement("div", {
    style: wrap,
    ...rest
  }, kindChip, titleEl, body, provEl);
}
Object.assign(__ds_scope, { Card });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Card/Card.jsx", error: String((e && e.message) || e) }); }

// components/CiteChip/CiteChip.jsx
try { (() => {
const React = window.React;

/**
 * CoralSwarm CiteChip — inline citation chip. Every claim links back to the
 * coral it came from. "soft" by default; "hot" for the active/hovered cite.
 */
function CiteChip({
  hot = false,
  children,
  href,
  onClick,
  style,
  ...rest
}) {
  const base = {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    minWidth: 16,
    minHeight: 16,
    padding: "1px 7px",
    borderRadius: "var(--radius-sm)",
    fontFamily: "var(--mono)",
    fontSize: 11,
    lineHeight: 1.3,
    textDecoration: "none",
    cursor: href || onClick ? "pointer" : "default",
    background: hot ? "var(--brand)" : "var(--brand-3)",
    color: hot ? "#fff" : "var(--brand-2)",
    ...style
  };
  const tag = href ? "a" : "span";
  return React.createElement(tag, {
    href,
    onClick,
    style: base,
    ...rest
  }, children);
}
Object.assign(__ds_scope, { CiteChip });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/CiteChip/CiteChip.jsx", error: String((e && e.message) || e) }); }

// components/Display/Display.jsx
try { (() => {
const React = window.React;

/**
 * CoralSwarm Display — Instrument Serif display heading. The human voice.
 * Wrap an emphasized word in <em> (rendered italic, coral) via children.
 * `size` accepts a px number or any CSS font-size string.
 */
function Display({
  as = "h2",
  size = 44,
  children,
  style,
  className,
  ...rest
}) {
  return React.createElement(as, {
    className: ["cs-display", className].filter(Boolean).join(" "),
    style: {
      fontFamily: "var(--display)",
      fontWeight: 400,
      lineHeight: 0.98,
      letterSpacing: "-0.02em",
      fontSize: typeof size === "number" ? `${size}px` : size,
      margin: 0,
      color: "inherit",
      textWrap: "balance",
      ...style
    },
    ...rest
  }, children);
}
Object.assign(__ds_scope, { Display });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Display/Display.jsx", error: String((e && e.message) || e) }); }

// components/Eyebrow/Eyebrow.jsx
try { (() => {
const React = window.React;

/**
 * CoralSwarm Eyebrow — mono uppercase kicker label above headings.
 */
function Eyebrow({
  as = "span",
  children,
  style,
  ...rest
}) {
  return React.createElement(as, {
    style: {
      fontFamily: "var(--mono)",
      fontSize: 11,
      letterSpacing: "0.22em",
      textTransform: "uppercase",
      color: "var(--ink-3)",
      display: "inline-block",
      ...style
    },
    ...rest
  }, children);
}
Object.assign(__ds_scope, { Eyebrow });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Eyebrow/Eyebrow.jsx", error: String((e && e.message) || e) }); }

// components/HueChip/HueChip.jsx
try { (() => {
const React = window.React;
const HUES = {
  coral: "var(--hue-coral)",
  pink: "var(--hue-pink)",
  amber: "var(--hue-amber)",
  teal: "var(--hue-teal)",
  plum: "var(--hue-plum)"
};
const HEX = {
  coral: "#F77764",
  pink: "#EB7290",
  amber: "#F4B860",
  teal: "#2A9D8F",
  plum: "#7C5295"
};
const MEANING = {
  coral: "decision",
  pink: "person",
  amber: "candidate",
  teal: "meeting",
  plum: "system"
};

/**
 * CoralSwarm HueChip — taxonomy legend item. Swatch + hue name + meaning + hex.
 * The five hues carry meaning, never decoration.
 */
function HueChip({
  hue = "coral",
  meaning,
  showCode = true,
  style,
  ...rest
}) {
  const color = HUES[hue] || hue;
  const m = meaning || MEANING[hue] || "";
  return React.createElement("span", {
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: 10,
      padding: "6px 12px 6px 8px",
      borderRadius: "var(--radius-pill)",
      background: "var(--bg)",
      border: "1px solid var(--line)",
      ...style
    },
    ...rest
  }, React.createElement("span", {
    style: {
      width: 18,
      height: 18,
      borderRadius: 6,
      background: color,
      flex: "none"
    }
  }), React.createElement("span", {
    style: {
      fontFamily: "var(--sans)",
      fontSize: 13,
      fontWeight: 500,
      color: "var(--ink)"
    }
  }, hue), m ? React.createElement("span", {
    style: {
      fontFamily: "var(--mono)",
      fontSize: 11,
      color: "var(--ink-3)",
      letterSpacing: "0.04em"
    }
  }, m) : null, showCode ? React.createElement("span", {
    style: {
      fontFamily: "var(--mono)",
      fontSize: 11,
      color: "var(--ink-3)",
      marginLeft: 2
    }
  }, HEX[hue] || "") : null);
}
Object.assign(__ds_scope, { HueChip });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/HueChip/HueChip.jsx", error: String((e && e.message) || e) }); }

// components/Input/Input.jsx
try { (() => {
const React = window.React;

/**
 * CoralSwarm Input — pill text field.
 * Cream surface, line border, coral focus ring. Invalid + disabled states.
 */
function Input({
  type = "text",
  size = "md",
  invalid = false,
  disabled = false,
  value,
  defaultValue,
  placeholder,
  onChange,
  ariaLabel,
  style,
  ...rest
}) {
  const sizes = {
    sm: {
      padding: "9px 14px",
      fontSize: 13
    },
    md: {
      padding: "12px 18px",
      fontSize: 14
    },
    lg: {
      padding: "16px 20px",
      fontSize: 15
    }
  };
  const [focus, setFocus] = React.useState(false);
  const borderColor = invalid ? "var(--bad)" : focus ? "var(--brand)" : "var(--line)";
  const base = {
    fontFamily: "var(--sans)",
    background: "var(--bg)",
    color: "var(--ink)",
    border: `1px solid ${borderColor}`,
    borderRadius: "var(--radius-pill)",
    outline: "none",
    lineHeight: 1.2,
    width: "100%",
    opacity: disabled ? 0.5 : 1,
    cursor: disabled ? "not-allowed" : "text",
    transition: "border-color var(--dur) var(--ease), box-shadow var(--dur) var(--ease)",
    boxShadow: focus && !invalid ? "var(--ring)" : invalid && focus ? "0 0 0 3px color-mix(in srgb, var(--bad) 30%, transparent)" : "none",
    ...sizes[size],
    ...style
  };
  return React.createElement("input", {
    type,
    value,
    defaultValue,
    placeholder,
    disabled,
    "aria-label": ariaLabel,
    "aria-invalid": invalid || undefined,
    onChange,
    onFocus: () => setFocus(true),
    onBlur: () => setFocus(false),
    style: base,
    ...rest
  });
}
Object.assign(__ds_scope, { Input });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Input/Input.jsx", error: String((e && e.message) || e) }); }

// components/Nav/Nav.jsx
try { (() => {
const React = window.React;

/**
 * CoralSwarm Nav — sticky top bar. Brand slot · links · CTA pill.
 * variant "cream" (default) blurs over the warm bg; "ink" for dark sections.
 */
function Nav({
  brand,
  links = [],
  cta,
  variant = "cream",
  sticky = true,
  style,
  ...rest
}) {
  const ink = variant === "ink";
  const bar = {
    position: sticky ? "sticky" : "relative",
    top: 0,
    zIndex: 50,
    background: ink ? "color-mix(in srgb, var(--ink) 88%, transparent)" : "color-mix(in srgb, var(--bg) 88%, transparent)",
    backdropFilter: "blur(12px)",
    WebkitBackdropFilter: "blur(12px)",
    borderBottom: ink ? "1px solid rgba(255,255,255,0.10)" : "1px solid color-mix(in srgb, var(--line) 60%, transparent)",
    color: ink ? "var(--bg)" : "var(--ink)",
    ...style
  };
  const inner = {
    maxWidth: 1280,
    margin: "0 auto",
    padding: "14px 32px",
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 28
  };
  const linksWrap = {
    display: "flex",
    gap: 28,
    fontSize: 14,
    color: ink ? "rgba(255,255,255,0.7)" : "var(--ink-2)"
  };
  function NavLink({
    link
  }) {
    const [hover, setHover] = React.useState(false);
    return React.createElement("a", {
      href: link.href || "#",
      onClick: link.onClick,
      onMouseEnter: () => setHover(true),
      onMouseLeave: () => setHover(false),
      style: {
        textDecoration: "none",
        transition: "color var(--dur) var(--ease)",
        color: hover ? ink ? "var(--bg)" : "var(--ink)" : "inherit"
      }
    }, link.label);
  }
  const [ctaHover, setCtaHover] = React.useState(false);
  const ctaEl = cta ? React.createElement("a", {
    href: cta.href || "#",
    onClick: cta.onClick,
    onMouseEnter: () => setCtaHover(true),
    onMouseLeave: () => setCtaHover(false),
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: 6,
      background: ink ? "var(--bg)" : "var(--ink)",
      color: ink ? "var(--ink)" : "var(--bg)",
      padding: "8px 16px",
      borderRadius: "var(--radius-pill)",
      fontSize: 13,
      fontWeight: 500,
      textDecoration: "none",
      transition: "transform var(--dur) var(--ease)",
      transform: ctaHover ? "translateY(-1px)" : "none"
    }
  }, cta.label) : null;
  return React.createElement("nav", {
    style: bar,
    ...rest
  }, React.createElement("div", {
    style: inner
  }, React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center"
    }
  }, brand), React.createElement("div", {
    style: {
      display: "flex",
      alignItems: "center",
      gap: 28
    }
  }, links.length ? React.createElement("div", {
    style: linksWrap
  }, links.map((link, i) => React.createElement(NavLink, {
    key: i,
    link
  }))) : null, ctaEl)));
}
Object.assign(__ds_scope, { Nav });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Nav/Nav.jsx", error: String((e && e.message) || e) }); }

// components/Pill/Pill.jsx
try { (() => {
const React = window.React;
const HUES = {
  coral: "var(--hue-coral)",
  pink: "var(--hue-pink)",
  amber: "var(--hue-amber)",
  teal: "var(--hue-teal)",
  plum: "var(--hue-plum)"
};

/**
 * CoralSwarm Pill — mono status pill on tinted surface with a hue dot.
 * Used for status / context flags (e.g. "now in private beta").
 */
function Pill({
  dot = "coral",
  children,
  style,
  ...rest
}) {
  const dotColor = dot ? HUES[dot] || dot : null;
  return React.createElement("span", {
    style: {
      display: "inline-flex",
      alignItems: "center",
      gap: 6,
      padding: "5px 12px",
      borderRadius: "var(--radius-pill)",
      background: "var(--bg-2)",
      border: "1px solid var(--line)",
      fontFamily: "var(--mono)",
      fontSize: 12,
      color: "var(--ink-2)",
      letterSpacing: "0.04em",
      ...style
    },
    ...rest
  }, dotColor ? React.createElement("span", {
    style: {
      width: 6,
      height: 6,
      borderRadius: "50%",
      background: dotColor,
      flex: "none"
    }
  }) : null, children);
}
Object.assign(__ds_scope, { Pill });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Pill/Pill.jsx", error: String((e && e.message) || e) }); }

// components/Section/Section.jsx
try { (() => {
const React = window.React;

/**
 * CoralSwarm Section — full-width surface band.
 * variant: "cream" | "tinted" | "ink". On ink, text flips to bg color.
 * Set `dotted` for the reef dot texture; `container` wraps children in the
 * 1280px max-width gutter.
 */
function Section({
  variant = "cream",
  dotted = false,
  container = true,
  pad = "88px 32px",
  children,
  style,
  innerStyle,
  ...rest
}) {
  const surfaces = {
    cream: {
      background: "var(--bg)",
      color: "var(--ink)"
    },
    tinted: {
      background: "var(--bg-2)",
      color: "var(--ink)"
    },
    ink: {
      background: "var(--ink)",
      color: "var(--bg)"
    }
  };
  const ink = variant === "ink";
  const dottedBg = dotted ? {
    backgroundImage: `radial-gradient(circle at 1px 1px, ${ink ? "rgba(255,255,255,0.08)" : "var(--line)"} 1px, transparent 0)`,
    backgroundSize: "22px 22px"
  } : null;
  const outer = {
    position: "relative",
    ...surfaces[variant],
    ...dottedBg,
    ...style
  };
  const inner = {
    maxWidth: container ? 1280 : "none",
    margin: "0 auto",
    padding: pad,
    position: "relative",
    ...innerStyle
  };
  return React.createElement("section", {
    style: outer,
    "data-variant": variant,
    ...rest
  }, React.createElement("div", {
    style: inner
  }, children));
}
Object.assign(__ds_scope, { Section });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Section/Section.jsx", error: String((e && e.message) || e) }); }

// components/Wordmark/Wordmark.jsx
try { (() => {
const React = window.React;
const HUES = ["coral", "pink", "amber", "teal", "plum"];
const hueVar = h => `var(--hue-${h})`;

/**
 * DotMandala — concentric rings of dots, each ring cycling the 5 brand hues.
 * The core mark; also backs the hero swarm.
 */
function DotMandala({
  size = 80,
  mono = false,
  color = "currentColor",
  density = 1,
  rotate = 0
}) {
  const rings = [{
    r: 0.0,
    n: 1,
    size: 0.085
  }, {
    r: 0.14,
    n: 6,
    size: 0.06
  }, {
    r: 0.25,
    n: 10,
    size: 0.05
  }, {
    r: 0.35,
    n: 14,
    size: 0.042
  }, {
    r: 0.45,
    n: 18,
    size: 0.034
  }];
  const dots = [];
  let k = 0;
  rings.forEach((ring, ri) => {
    const n = Math.max(1, Math.round(ring.n * density));
    for (let i = 0; i < n; i++) {
      const a = ri === 0 ? 0 : i / n * Math.PI * 2 + ri * 0.4 + rotate * Math.PI / 180;
      const x = 0.5 + Math.cos(a) * ring.r;
      const y = 0.5 + Math.sin(a) * ring.r;
      const fill = mono ? color : hueVar(HUES[(i + ri) % HUES.length]);
      dots.push(React.createElement("circle", {
        key: k++,
        cx: x * size,
        cy: y * size,
        r: ring.size * size,
        fill
      }));
    }
  });
  return React.createElement("svg", {
    viewBox: `0 0 ${size} ${size}`,
    width: size,
    height: size,
    style: {
      display: "block"
    }
  }, dots);
}

/**
 * Wordmark — PRIMARY lockup. Sans "coral" + DotMandala bridge + "swarm".
 * Geist 600, letter-spacing -0.045em. Never stretch, recolor, or box.
 */
function Wordmark({
  size = 56,
  mono = false,
  color = "currentColor",
  style
}) {
  const dotSize = size * 0.78;
  return React.createElement("span", {
    style: {
      display: "inline-flex",
      alignItems: "center",
      fontFamily: "var(--sans)",
      fontWeight: 600,
      fontSize: size,
      lineHeight: 1,
      letterSpacing: "-0.045em",
      color,
      whiteSpace: "nowrap",
      ...style
    }
  }, React.createElement("span", null, "coral"), React.createElement("span", {
    style: {
      display: "inline-flex",
      width: dotSize,
      height: dotSize,
      margin: `0 ${size * 0.04}px`
    }
  }, React.createElement(DotMandala, {
    size: dotSize,
    mono,
    color,
    density: 0.75
  })), React.createElement("span", null, "swarm"));
}
Object.assign(__ds_scope, { DotMandala, Wordmark });
})(); } catch (e) { __ds_ns.__errors.push({ path: "components/Wordmark/Wordmark.jsx", error: String((e && e.message) || e) }); }

__ds_ns.Badge = __ds_scope.Badge;

__ds_ns.Button = __ds_scope.Button;

__ds_ns.Card = __ds_scope.Card;

__ds_ns.CiteChip = __ds_scope.CiteChip;

__ds_ns.Display = __ds_scope.Display;

__ds_ns.Eyebrow = __ds_scope.Eyebrow;

__ds_ns.HueChip = __ds_scope.HueChip;

__ds_ns.Input = __ds_scope.Input;

__ds_ns.Nav = __ds_scope.Nav;

__ds_ns.Pill = __ds_scope.Pill;

__ds_ns.Section = __ds_scope.Section;

__ds_ns.DotMandala = __ds_scope.DotMandala;

__ds_ns.Wordmark = __ds_scope.Wordmark;

})();
