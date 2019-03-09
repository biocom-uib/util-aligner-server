from scores import compute_score


async def compute_and_write_score(db, net, specie, score, score_threshold):
    score_dict = compute_score(score, net)
    await db.write_score(db, specie, score, score_dict, score_threshold)
