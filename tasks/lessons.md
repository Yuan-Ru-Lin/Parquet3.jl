# Lessons

- Reference PDFs we do not own (papers, specifications) are not committed: link to them in
  the docs and keep local copies in a gitignored folder (`papers/`). A file committed to an
  unpushed branch still becomes public with the first push, and removing it afterwards
  means rewriting history. (2026-10-04: `papers/36632.pdf` was tracked on 2026-10-03 after
  I had proposed ignoring it, and then had to be removed from history before any push.)
- Only ignore generated files, scratch environments, large local data, and local copies of
  reference material. If I can't tell what an untracked file is, ask what it is before
  proposing to ignore or to commit it.
