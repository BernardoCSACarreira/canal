const PARAM = 'tierUxPrototype'

/** Interim tier / SLO copy and CAN-28 token-aligned badges; ship defaults when the flag is off. */
export function isTierUxPrototypeActive(): boolean {
  if (import.meta.env.VITE_TIER_UX_PROTOTYPE === 'true') return true
  return new URLSearchParams(window.location.search).get(PARAM) === '1'
}
