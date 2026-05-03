const PARAM = 'operatorWizard'

export function isWizardDeepLinkActive(): boolean {
  return new URLSearchParams(window.location.search).get(PARAM) === '1'
}

/** Sidebar entry: dev builds or explicit Vite flag for production demos. */
export function shouldOfferWizardNav(): boolean {
  return (
    import.meta.env.DEV || import.meta.env.VITE_OPERATOR_WIZARD === 'true'
  )
}

export function openWizardInUrl(): void {
  const u = new URL(window.location.href)
  u.searchParams.set(PARAM, '1')
  window.history.pushState({}, '', u)
}

export function clearWizardFromUrl(): void {
  const u = new URL(window.location.href)
  u.searchParams.delete(PARAM)
  window.history.pushState({}, '', u)
}
