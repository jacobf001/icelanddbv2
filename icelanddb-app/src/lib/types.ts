export type TeamRow = {
  ksi_team_id: string;

  // keep both so old/new code works
  name: string | null;
  team_name: string | null;
};


export type TeamSeasonSummary = {
  season_year: number;
  ksi_team_id: string;
  team_name: string | null;
  matches_played: number;
  wins: number;
  draws: number;
  losses: number;
  points: number;
  goals_for: number;
  goals_against: number;
  goal_diff: number;
  yellows: number;
  reds: number;
};

export type PlayerSeasonRow = {
  season_year: number;
  ksi_team_id: string;
  ksi_player_id: string;
  player_name: string | null;
  matches_played: number;
  starts: number;
  minutes: number;
  goals: number;
  yellows: number;
  reds: number;
};

export type LikelyXIPlayer = {
  ksi_player_id: string;
  player_name: string | null;
  minutes: number;
  starts: number;
  goals: number;
};

export type MatchPreviewResponse = {
  season_year: number;
  home_team_ksi_id: string;
  away_team_ksi_id: string;
  home: {
    summary: TeamSeasonSummary | null;
    topPlayers: PlayerSeasonRow[];
    likelyXI: LikelyXIPlayer[];
  };
  away: {
    summary: TeamSeasonSummary | null;
    topPlayers: PlayerSeasonRow[];
    likelyXI: LikelyXIPlayer[];
  };
};
