-- PAIRED WITH: tenant 0236 (read_model_queue_priority).
-- Partial indexes match the oldest-row fairness pass and empty-first priority pass.
-- Keep the existing due/debounce index for other readers. New small cache-table
-- indexes use ordinary CREATE INDEX, as in the existing read-model migrations.
CREATE INDEX read_models_oldest_dirty_idx
 ON read_models(dirty_at,model,scope_key)
 WHERE dirty_at IS NOT NULL;
CREATE INDEX read_models_priority_idx
 ON read_models((computed_at IS NULL) DESC,(failed_attempts>0),dirty_at,model,scope_key)
 WHERE dirty_at IS NOT NULL;
