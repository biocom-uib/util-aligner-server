from asyncio import get_event_loop, gather
from server.analyzer.interactor import compute_and_write_score
from server.analyzer.scores import SCORES
from sources.stringdb import StringDB


async def main(specie, score_threshold):
    db = StringDB()
    net = await db.get_network(specie, score_threshold).to_networkx()
    tasks = [compute_and_write_score(db, net, score, score_threshold)
             for score in SCORES]
    gather(*tasks)


if __name__ == '__main__':
    loop = get_event_loop()
    specie = 6239
    score_threshold = {score: 0 for score in StringDB.EVIDENCE_SCORE_TYPES}
    for i in range(11):
        score_threshold['database_score'] = i*100
        loop.run_until_complete(main(specie, score_threshold))
