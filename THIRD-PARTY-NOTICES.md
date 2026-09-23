# Third-party notices

Saga's test suite adapts a subset of test scenarios from
[`reactor`](https://github.com/ash-project/reactor) `v1.0.6`
(commit `e4ddc9e438f08562e9fb31b08255a86f4bade8ef`), used here as a behavioral
oracle. `oracle/reactor/` also depends on the `reactor` hex package directly
to run the same scenarios for a differential comparison. Reactor is
distributed under the MIT license:

```
SPDX-FileCopyrightText: 2023 James Harton, Zach Daniel, Alembic Pty and contributors
SPDX-FileCopyrightText: 2023 reactor contributors <https://github.com/ash-project/reactor/graphs/contributors>

SPDX-License-Identifier: MIT
```

```
MIT License

Copyright (c) 2023 James Harton, Zach Daniel, Alembic Pty and contributors

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the
following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO
EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE
USE OR OTHER DEALINGS IN THE SOFTWARE.
```

See [`PROVENANCE.md`](PROVENANCE.md) for which Saga tests were adapted from
which upstream Reactor tests, and which behaviors are deliberately different.
