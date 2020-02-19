from asyncio import get_event_loop, gather
from server.analyzer.interactor import compute_and_write_score
from server.analyzer.scores import SCORES
from server.sources.stringdb import StringDB


async def main(species, score_threshold):
    async with StringDB() as db:
        net = await db.get_network(species, score_threshold)
        net_nx = net.to_networkx()

        tasks = [compute_and_write_score(db, net_nx, species, score, score_threshold)
                 for score in SCORES]
        gather(*tasks)


if __name__ == '__main__':
    loop = get_event_loop()
    species = 6239
    for i in range(11):
        loop.run_until_complete(main(species, {'database_score': i*100}))
