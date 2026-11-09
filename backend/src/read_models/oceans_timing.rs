//! One fixed-field numeric receipt per OceansIndex body, including interrupted
//! bodies. No identity, errors, SQL, parameters or payload can enter this type.
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
pub(super) enum Phase {
    Roster,
    Metadata,
    Counts,
    RoleOrgFlag,
    Governance,
    TeamPeople,
    Archived,
}

pub(super) struct Timing {
    started: Instant,
    active: Option<(Phase, Instant)>,
    elapsed: [Duration; 7],
    counts_max: Duration,
    counts_n: u64,
    pub graphs_n: usize,
    pub members_n: usize,
    pub team_rows_n: usize,
    pub completed: u64,
}
impl Timing {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            active: None,
            elapsed: [Duration::ZERO; 7],
            counts_max: Duration::ZERO,
            counts_n: 0,
            graphs_n: 0,
            members_n: 0,
            team_rows_n: 0,
            completed: 0,
        }
    }
    pub fn enter(&mut self, phase: Phase) {
        self.finish();
        self.active = Some((phase, Instant::now()));
    }
    pub fn finish(&mut self) {
        if let Some((phase, start)) = self.active.take() {
            let elapsed = start.elapsed();
            self.elapsed[phase as usize] += elapsed;
            if matches!(phase, Phase::Counts) {
                self.counts_n += 1;
                self.counts_max = self.counts_max.max(elapsed);
            }
        }
    }
}
impl Drop for Timing {
    fn drop(&mut self) {
        // Also runs on SQL error, panic or cancellation; never format the error.
        self.finish();
        let total = self.started.elapsed();
        let sum: Duration = self.elapsed.iter().sum();
        let ms = |d: Duration| d.as_secs_f64() * 1000.;
        tracing::info!(
            completed = self.completed,
            body_ms = ms(total),
            roster_ms = ms(self.elapsed[Phase::Roster as usize]),
            metadata_ms = ms(self.elapsed[Phase::Metadata as usize]),
            counts_n = self.counts_n,
            counts_sum_ms = ms(self.elapsed[Phase::Counts as usize]),
            counts_max_ms = ms(self.counts_max),
            role_org_flag_ms = ms(self.elapsed[Phase::RoleOrgFlag as usize]),
            governance_ms = ms(self.elapsed[Phase::Governance as usize]),
            team_people_ms = ms(self.elapsed[Phase::TeamPeople as usize]),
            archived_ms = ms(self.elapsed[Phase::Archived as usize]),
            residual_ms = ms(total.saturating_sub(sum)),
            graphs_n = self.graphs_n,
            members_n = self.members_n,
            team_rows_n = self.team_rows_n,
            "oceans index phases completed"
        );
    }
}
