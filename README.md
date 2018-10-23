# Search engine project

In this project we have created a search engine using spark and hadoop for indexing, cassandra for storage and a client in spark. It was a school project for the course Data Intensive Computing Platforms in KTH.

It parses keywords from web sites, to filter websites and uses links to create a [PageRank](https://en.wikipedia.org/wiki/PageRank). The scoring system for sorting websites is a combination of relevance (number of times keyword appears on per page) and importance (PageRank).

We recomend using web crawler data from [Common Crawl](http://commoncrawl.org). With only a very small subset the search engine performed surprisingly well, however with more computing power or / and time you could index a larger part of the internet.

## Screenshot

![screenshot](ScreenShot.png)

## License [![MIT license][license-img]][license-url]

> The [`MIT`][license-url] License (MIT)
>
> Copyright (c) 2018 Hannes Rabo
> Copyright (c) 2018 Julius Recep Colliander Celik
>
> Permission is hereby granted, free of charge, to any person obtaining a copy
> of this software and associated documentation files (the "Software"), to deal
> in the Software without restriction, including without limitation the rights
> to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
> copies of the Software, and to permit persons to whom the Software is
> furnished to do so, subject to the following conditions:
>
> The above copyright notice and this permission notice shall be included in all
> copies or substantial portions of the Software.
>
> THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
> IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
> FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
> AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
> LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
> OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
> SOFTWARE.
>
> For further details see [LICENSE](LICENSE) file.

[license-img]: https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square
[license-url]: https://github.com/juliuscc/cputemp-macos/blob/master/LICENSE
