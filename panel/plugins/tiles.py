import io
import importlib

from collections import Counter
from pathlib import Path

from datashader import Canvas
from datashader.tiles import TileRenderer, MercatorTileDefinition

from tornado.web import RequestHandler, HTTPError

CACHE = Path('tile_cache')

class TileProvider:

    def __init__(self, provider):
        self._provider = provider
        self._module = importlib.import_module(provider)
        self._tile_size = getattr(self._module, 'tile_size', 256)
        self._definition = MercatorTileDefinition(
            self._module.x_range,
            self._module.y_range,
            self._tile_size
        )

    async def get(self, x, y, z):
        path = CACHE / self._provider / str(x) / str(y) / f"{z}.png"
        if path.exists():
            with open(path, 'rb') as f:
                return f.read()
        path.parent.mkdir(parents=True, exist_ok=True)
        x0, y0, x1, y1 = self._definition.get_tile_meters(x, y, z)
        agg = self._module.aggregate(
            x_range=(x0, x1), y_range=(y0, y1),
            width=self._tile_size, height=self._tile_size
        )
        img = agg.to_pil()
        bio = io.BytesIO()
        img.save(bio, 'png')
        img.save(path)
        bio.seek(0)
        return bio.read()


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
        print(path)
        if not path.endswith('.png'):
            raise HTTPError(400, 'Must request png')
        elif not Counter(path)['/'] <= 3:
            raise HTTPError(400, 'Request must be of format provider/x/y/z.png')
        *provider, x, y, z = path.split('/')
        provider = self._get_provider('.'.join(provider))
        x, y, z = int(x), int(y), int(z.split('.')[0])
        self.set_header('Content-Type', 'image/png')
        self.write(await provider.get(x, y, z))
