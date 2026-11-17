import raio
from raio import Node, S, task


class Ticker(Node):
    @task
    async def tick(self):
        for i in range(1, 5):
            now = await raio.sleep(1 * S)
            print(f"tick {i} at {now}")


raio.run(Ticker())
