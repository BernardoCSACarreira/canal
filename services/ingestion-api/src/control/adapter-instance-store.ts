import { getPipelineSummaryRead, type AdapterInstancePlaceholder, type CatalogTier } from "./read-models.js";

export type AdapterInstanceRecord = {
  id: string;
  catalogAdapterId: string;
  catalogTier: CatalogTier;
  stageKey: string;
  displayName: string;
  operatorLabel: string | null;
  createdAt: string;
  updatedAt: string;
};

function catalogPlaceholder(catalogAdapterId: string): AdapterInstancePlaceholder | undefined {
  return getPipelineSummaryRead().adapterInstances.find((a) => a.id === catalogAdapterId);
}

export type AdapterInstanceStore = ReturnType<typeof createAdapterInstanceStore>;

export function createAdapterInstanceStore() {
  const byId = new Map<string, AdapterInstanceRecord>();

  return {
    list(): AdapterInstanceRecord[] {
      return [...byId.values()].sort((a, b) => a.createdAt.localeCompare(b.createdAt));
    },

    get(id: string): AdapterInstanceRecord | undefined {
      return byId.get(id);
    },

    create(input: { catalogAdapterId: string; operatorLabel?: string | null }):
      | { ok: true; record: AdapterInstanceRecord }
      | { ok: false; message: string } {
      const ph = catalogPlaceholder(input.catalogAdapterId);
      if (!ph) {
        return {
          ok: false,
          message:
            "`catalogAdapterId` is not a known pipeline adapter placeholder — tier is taken only from the control read model catalog.",
        };
      }
      const now = new Date().toISOString();
      const record: AdapterInstanceRecord = {
        id: crypto.randomUUID(),
        catalogAdapterId: ph.id,
        catalogTier: ph.catalogTier,
        stageKey: ph.stageKey,
        displayName: ph.displayName,
        operatorLabel:
          typeof input.operatorLabel === "string" && input.operatorLabel.trim().length > 0
            ? input.operatorLabel.trim()
            : null,
        createdAt: now,
        updatedAt: now,
      };
      byId.set(record.id, record);
      return { ok: true, record };
    },

    update(
      id: string,
      patch: { catalogAdapterId?: string; operatorLabel?: string | null },
    ):
      | { ok: true; record: AdapterInstanceRecord }
      | { ok: false; notFound: true }
      | { ok: false; message: string } {
      const existing = byId.get(id);
      if (!existing) return { ok: false, notFound: true };

      if (patch.catalogAdapterId !== undefined) {
        const ph = catalogPlaceholder(patch.catalogAdapterId);
        if (!ph) {
          return {
            ok: false,
            message:
              "`catalogAdapterId` is not a known pipeline adapter placeholder — tier is taken only from the control read model catalog.",
          };
        }
        existing.catalogAdapterId = ph.id;
        existing.catalogTier = ph.catalogTier;
        existing.stageKey = ph.stageKey;
        existing.displayName = ph.displayName;
      }

      if (patch.operatorLabel !== undefined) {
        existing.operatorLabel =
          typeof patch.operatorLabel === "string" && patch.operatorLabel.trim().length > 0
            ? patch.operatorLabel.trim()
            : null;
      }

      existing.updatedAt = new Date().toISOString();
      return { ok: true, record: existing };
    },

    delete(id: string): boolean {
      return byId.delete(id);
    },
  };
}
