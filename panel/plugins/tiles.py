import io
import importlib

from asyncio import Future
from collections import Counter
from pathlib import Path

from datashader import Canvas
from datashader.tiles import MercatorTileDefinition, gen_super_tiles

from tornado.web import RequestHandler, HTTPError

CACHE = Path('tile_cache')

class TileProvider:

    SUPERTILE_SIZE = 3

    _futures = {}

    def __init__(self, provider):
        self._provider = provider
        self._module = importlib.import_module(provider)
        self._tile_size = getattr(self._module, 'tile_size', 256)
        self._definition = MercatorTileDefinition(
            self._module.x_range,
            self._module.y_range,
            self._tile_size
        )

    async def _prepare_supertile(self, x, y, z):
        sts = self.SUPERTILE_SIZE // 2
        zmax = (2**z)-1
        xmin, ymin = max(0, x-sts), max(0, y-sts)
        xmax, ymax = min(zmax, x+sts), min(zmax, y+sts)
        for xi in range(xmin, xmax+1): 
            for yi in range(ymin, ymax+1):
                self._futures[(xi, yi, z)] = Future()
        x0, y0, _, _ = self._definition.get_tile_meters(xmin, ymax, z)
        _, _, x1, y1 = self._definition.get_tile_meters(xmax, ymin, z)
        agg = self._module.aggregate(
            x_range=(x0, x1), y_range=(y0, y1),
            width=self._tile_size*((xmax-xmin)+1),
            height=self._tile_size*((ymax-ymin)+1)
        )
        for xi in range(xmin, xmax+1): 
            for yi in range(ymin, ymax+1):
                path = CACHE / self._provider / str(xi) / str(yi) / f"{z}.png"
                path.parent.mkdir(parents=True, exist_ok=True)
                x0, y0, x1, y1 = self._definition.get_tile_meters(xi, yi, z)
                img = agg.loc[y0:y1, x0:x1].to_pil()
                bio = io.BytesIO()
                img.save(bio, 'png')
                bio.seek(0)
                self._futures[(xi, yi, z)].set_result(bio.read())
                img.save(path)

    async def get(self, x, y, z):
        path = CACHE / self._provider / str(x) / str(y) / f"{z}.png"
        if path.exists():
            with open(path, 'rb') as f:
                return f.read()
        if (x, y, z) not in self._futures:
            await self._prepare_supertile(x, y, z)
        return (await self._futures.pop((x, y, z)))

class TileHandler(RequestHandler):

    _providers = {}

    @classmethod
    def _get_provider(cls, provider):
        if provider not in cls._providers:
            cls._providers[provider] = pobj = TileProvider(provider)
        else:
            pobj = cls._providers[provider]
        return pobj

    async def get(self, path):
        if not path.endswith('.png'):
            raise HTTPError(400, 'Must request png')
        elif not Counter(path)['/'] <= 3:
            raise HTTPError(400, 'Request must be of format provider/x/y/z.png')
        *provider, x, y, z = path.split('/')
        provider = self._get_provider('.'.join(provider))
        x, y, z = int(x), int(y), int(z.split('.')[0])
        self.set_header('Content-Type', 'image/png')
        self.write(await provider.get(x, y, z))


ROUTES = [('/panel_tiles/(.*)', TileHandler, {})]
