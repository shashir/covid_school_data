class ProgressBar(object):
  def __init__(self, total, progress=0, note="", bar_length=50):
    self.total = total
    self.progress = progress
    self.bar_length = bar_length
    self.note = note
    self.display()
    self.is_complete = False

  def display(self):
    progress_frac = float(self.progress) / self.total
    progress_bar = int(progress_frac * self.bar_length)
    remainder = self.bar_length - progress_bar
    print("\r[%s%s] %d%%" % (
        "#" * progress_bar,
        " " * remainder,
        int(progress_frac * 100)), self.note, end="")
    if progress_frac >= 1.0:
      if not self.is_complete:
        self.close()
      self.is_complete = True

  def increment(self, note=None):
    if note:
      self.note = note
    self.progress += 1
    self.display()

  def set_note(self, note=""):
    self.note = note
    self.display()

  def set_progress(self, progress, note=None):
    if note:
      self.note = note
    self.progress = progress
    self.display()

  def complete(self):
    if not self.is_complete:
      self.set_progress(self.total)

  def close(self):
    print()
