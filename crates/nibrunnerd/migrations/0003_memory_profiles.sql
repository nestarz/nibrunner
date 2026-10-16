-- The highest working set each deployment was measured at, under the resources it was measured
-- with: what a later cold start or wake of the same deployment is admitted against before it can
-- be measured again. Kept for a bounded time after the deployment is gone, because a control plane
-- that brings back a revision expects it to start the way it ran.
create table memory_profiles (
    app_id         text    not null,
    deployment_id  text    not null,
    vcpu_count     integer not null check (vcpu_count > 0),
    memory_mib     integer not null check (memory_mib > 0),
    peak_bytes     integer not null check (peak_bytes > 0),
    observed_at_ms integer not null,
    primary key (app_id, deployment_id)
) strict;
