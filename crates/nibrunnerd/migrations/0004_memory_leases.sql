-- Memory leased through memory.sock for work outside the microVMs, such as builds. Written before
-- a grant is answered and read back before anything is admitted after a restart, because that
-- work can outlive the daemon. A lease is only good for the boot it was granted in: its owner is
-- a process, known by its pid and the clock tick it started at.
create table memory_leases (
    lease_id                text    not null primary key,
    boot_id                 text    not null,
    unit                    text    not null,
    memory_mib              integer not null check (memory_mib > 0),
    minimum_mib             integer check (minimum_mib > 0),
    preferred_mib           integer check (preferred_mib > 0),
    owner_pid               integer not null check (owner_pid > 0),
    owner_start_ticks       integer not null,
    owner_cgroup            text    not null,
    owner_group_membership  text,
    owner_group_device      integer,
    owner_group_inode       integer,
    budget_group_membership text,
    budget_group_device     integer,
    budget_group_inode      integer
) strict;
