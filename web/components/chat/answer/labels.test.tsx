import { describe, it, expect } from "vitest";
import { JEV_VERDICTS, JEV_VERDICT_META } from "@/lib/graphSurfaces";
import { sourceButtonLabel, verdictCopy } from "./labels";
describe("citation labels",()=>{
 it("keeps the stable source selector",()=>expect(sourceButtonLabel(1,"Plan",null)).toBe("Open source 1: Plan"));
 for(const verdict of JEV_VERDICTS) it(verdict,()=>{
 expect(verdictCopy(verdict)?.description).toBe(JEV_VERDICT_META[verdict].description);
 expect(sourceButtonLabel(2,"Plan",verdict)).toContain(`Jev: ${JEV_VERDICT_META[verdict].label}`);
 });
});
