from mrjob.job import MRJob

class AvgApprovalAge(MRJob):
  def mapper(self, _, line):
    entries = line.split(',')
    if entries[2] == 'credit_score':
      return
    age = int(entries[6])
    approved = int(entries[7])
    yield (approved, age)

  def reducer(self, key, values):
    values = list(values)
    yield (key, sum(values)/len(values))

if __name__ == "__main__":
  AvgApprovalAge.run()