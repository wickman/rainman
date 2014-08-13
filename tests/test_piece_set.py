from rainman.bitfield import Bitfield
from rainman.piece_set import PieceSet


def test_pieceset_basic():
  ps = PieceSet(4)
  ps.add_one(0)  # 0: 1
  assert ps.rarest() == [0]
  ps.add_one(0)  # 0: 2
  assert ps.rarest() == [0]
  ps.add_one(1)  # 0: 2, 1: 1
  assert ps.rarest() == [1, 0]
  ps.add_one(1)  # 0: 2, 1: 2
  assert ps.rarest() == [0, 1]
  ps.add_one(3)  # 0: 2, 1: 2, 3: 1
  assert ps.rarest() == [3, 0, 1]

  bf = Bitfield(4)
  bf[1] = True
  ps.remove(bf)  # 0: 2, 1: 1, 3: 1
  assert ps.rarest() == [1, 3, 0]

  bf = Bitfield(4)
  bf[3] = True
  ps.remove(bf)
  assert ps.rarest() == [1, 0]

