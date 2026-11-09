// The CoralSwarm brand mark — reused verbatim (same viewBox, same 36 circles,
// same fills) from the header of widget-design/Main.dc.html (and every other
// approved artboard). Never re-derive this; copy exactly if it ever needs to
// move.

interface MandalaProps {
  size?: number;
}

export function Mandala({ size = 18 }: MandalaProps) {
  return (
    <svg viewBox="0 0 1 1" width={size} height={size} style={{ flex: "none", display: "block" }}>
      <circle cx="0.5000" cy="0.5000" r="0.085" fill="#F77764" />
      <circle cx="0.6289" cy="0.5545" r="0.060" fill="#EB7290" />
      <circle cx="0.4455" cy="0.6289" r="0.060" fill="#F4B860" />
      <circle cx="0.3711" cy="0.4455" r="0.060" fill="#2A9D8F" />
      <circle cx="0.5545" cy="0.3711" r="0.060" fill="#7C5295" />
      <circle cx="0.6742" cy="0.6793" r="0.050" fill="#F4B860" />
      <circle cx="0.4963" cy="0.7500" r="0.050" fill="#2A9D8F" />
      <circle cx="0.3207" cy="0.6742" r="0.050" fill="#7C5295" />
      <circle cx="0.2500" cy="0.4963" r="0.050" fill="#F77764" />
      <circle cx="0.3258" cy="0.3207" r="0.050" fill="#EB7290" />
      <circle cx="0.5037" cy="0.2500" r="0.050" fill="#F4B860" />
      <circle cx="0.6793" cy="0.3258" r="0.050" fill="#2A9D8F" />
      <circle cx="0.7500" cy="0.5037" r="0.050" fill="#7C5295" />
      <circle cx="0.6268" cy="0.8262" r="0.042" fill="#2A9D8F" />
      <circle cx="0.4109" cy="0.8385" r="0.042" fill="#7C5295" />
      <circle cx="0.2289" cy="0.7214" r="0.042" fill="#F77764" />
      <circle cx="0.1506" cy="0.5198" r="0.042" fill="#EB7290" />
      <circle cx="0.2057" cy="0.3106" r="0.042" fill="#F4B860" />
      <circle cx="0.3732" cy="0.1738" r="0.042" fill="#2A9D8F" />
      <circle cx="0.5891" cy="0.1615" r="0.042" fill="#7C5295" />
      <circle cx="0.7711" cy="0.2786" r="0.042" fill="#F77764" />
      <circle cx="0.8494" cy="0.4802" r="0.042" fill="#EB7290" />
      <circle cx="0.7943" cy="0.6894" r="0.042" fill="#F4B860" />
      <circle cx="0.4869" cy="0.9498" r="0.034" fill="#7C5295" />
      <circle cx="0.2930" cy="0.8996" r="0.034" fill="#F77764" />
      <circle cx="0.1401" cy="0.7702" r="0.034" fill="#EB7290" />
      <circle cx="0.0585" cy="0.5873" r="0.034" fill="#F4B860" />
      <circle cx="0.0644" cy="0.3871" r="0.034" fill="#2A9D8F" />
      <circle cx="0.1565" cy="0.2093" r="0.034" fill="#7C5295" />
      <circle cx="0.3167" cy="0.0890" r="0.034" fill="#F77764" />
      <circle cx="0.5131" cy="0.0502" r="0.034" fill="#EB7290" />
      <circle cx="0.7070" cy="0.1004" r="0.034" fill="#F4B860" />
      <circle cx="0.8599" cy="0.2298" r="0.034" fill="#2A9D8F" />
      <circle cx="0.9415" cy="0.4127" r="0.034" fill="#7C5295" />
      <circle cx="0.9356" cy="0.6129" r="0.034" fill="#F77764" />
      <circle cx="0.8435" cy="0.7907" r="0.034" fill="#EB7290" />
      <circle cx="0.6833" cy="0.9110" r="0.034" fill="#F4B860" />
    </svg>
  );
}
