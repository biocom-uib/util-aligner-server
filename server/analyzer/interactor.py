from server.analyzer.scores import compute_score


async def compute_and_write_score(db, net_nx, species, score, score_threshold):
    score_dict = compute_score(score, net_nx, species)
    await db.write_analysis_score(db, species, score, score_dict, score_threshold)
