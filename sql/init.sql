-- The application migration command owns all table definitions.
-- Run python -m paylens.replay.ops migrate with the migration account.
-- This role can be granted to a managed read-only analytics login.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'paylens_analytics') THEN
        CREATE ROLE paylens_analytics NOLOGIN;
    END IF;
END $$;
GRANT USAGE ON SCHEMA public TO paylens_analytics;
GRANT SELECT ON payment_attempts, payment_events, experiment_assignments, assignment_conflicts,
    experiment_protocols, data_watermarks, deployments TO paylens_analytics;
