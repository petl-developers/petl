"""
Test cachetag methods on transform views.

"""

from petl.util import randomtable
from petl.transform import RenameView, CutView, CatView, FieldConvertView,\
    ExtendView, RowSliceView, TailView, SortView, MeltView, RecastView,\
    DuplicatesView, ConflictsView, ComplementView, CaptureView, SplitView,\
    RowSelectView, FieldSelectView, FieldMapView, RowReduceView, \
    AggregateView, RangeRowReduceView, RangeAggregateView,\
    RowMapView, RowMapManyView, SetHeaderView,\
    ExtendHeaderView, PushHeaderView, SkipView, UnpackView, JoinView,\
    ImplicitJoinView, CrossJoinView, AntiJoinView, ImplicitAntiJoinView

def test_rename():
    src = randomtable(2, 10)
    tbl = RenameView(src, {'f0': 'foo', 'f1': 'bar'})
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_cut():
    src = randomtable(4, 10)
    tbl = CutView(src, 'f0', 'f2')
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_cat():
    src = randomtable(4, 10)
    tbl = CatView([src])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_fieldconvert():
    src = randomtable(4, 10)
    converters = {'f0': int, 'f1': {'foo': 'bar'}, 'f2': ['replace', 'a', 'aa']}
    tbl = FieldConvertView(src, converters)
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_extend():
    src = randomtable(2, 10)
    tbl = ExtendView(src, 'ext', lambda rec: rec['f0'] + rec['f1'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_rowslice():
    src = randomtable(2, 10)
    tbl = RowSliceView(src, 1, 3)
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_tail():
    src = randomtable(2, 10)
    tbl = TailView(src, 3)
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_sort():
    src = randomtable(2, 10)
    tbl = SortView(src)
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_melt():
    src = randomtable(4, 10)
    tbl = MeltView(src, key='f0')
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_recast():
    src = randomtable(4, 10)
    tbl = RecastView(src, variablefield='f1', valuefield='f2')
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_duplicates():
    src = randomtable(4, 10)
    tbl = DuplicatesView(src, key='f0')
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_conflicts():
    src = randomtable(4, 10)
    tbl = ConflictsView(src, key='f0')
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_complement():
    srca = randomtable(4, 10)
    srcb = randomtable(4, 10)
    tbl = ComplementView(srca, srcb)
    before = tbl.cachetag()
    srca.reseed()
    after = tbl.cachetag()
    assert before != after

def test_capture():
    src = randomtable(4, 10)
    tbl = CaptureView(src, 'f0', 'foo', newfields=('a', 'b'))
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_split():
    src = randomtable(4, 10)
    tbl = SplitView(src, 'f0', 'foo', newfields=('a', 'b'))
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_select():
    src = randomtable(4, 10)
    tbl = RowSelectView(src, lambda rec: rec['f0'] > rec['f1'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_selectf():
    src = randomtable(4, 10)
    tbl = FieldSelectView(src, 'f0', lambda v: v > 0)
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_fieldmap():
    src = randomtable(4, 10)
    tbl = FieldMapView(src)
    tbl['F0'] = 'f0'
    tbl['F1'] = 'f1', {True: False, False: True}
    tbl['F2'] = 'f2', lambda v: v**2
    tbl['F3'] = lambda rec: rec['f0'] * rec['f1']
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_rowreduce():
    src = randomtable(4, 10)
    redu = lambda key, rows: [key, sum([r[1] for r in rows])]
    tbl = RowReduceView(src, key='f0', reducer=redu, fields=['key', 'sum'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_aggregate():
    src = randomtable(4, 10)
    tbl = AggregateView(src, 'f0')
    tbl['a1'] = 'f1', sum
    tbl['a2'] = 'f2', max
    tbl['a3'] = 'f3'
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_rangerowreduce():
    src = randomtable(4, 10)
    redu = lambda minv, maxv, rows: [minv, maxv, sum([r[1] for r in rows])]
    tbl = RangeRowReduceView(src, 'f0', 0.2, reducer=redu, fields=['min', 'max', 'sum'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_rangeaggregate():
    src = randomtable(4, 10)
    tbl = RangeAggregateView(src, 'f0', 0.2)
    tbl['a1'] = 'f1', sum
    tbl['a2'] = 'f2', max
    tbl['a3'] = 'f3'
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_rowmap():
    src = randomtable(4, 10)
    tbl = RowMapView(src, lambda row: [row[0] + row[1], row[2]], fields=['foo', 'bar'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_rowmapmany():
    src = randomtable(4, 10)
    def mapf(row):
        yield [row[0] + row[1], row[2]]
    tbl = RowMapManyView(src, mapf, fields=['foo', 'bar'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_setheader():
    src = randomtable(2, 10)
    tbl = SetHeaderView(src, ['foo', 'bar'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_extendheader():
    src = randomtable(2, 10)
    tbl = ExtendHeaderView(src, ['foo', 'bar'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after

def test_pushheader():
    src = randomtable(2, 10)
    tbl = PushHeaderView(src, ['foo', 'bar'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_skip():
    src = randomtable(2, 10)
    tbl = SkipView(src, 2)
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_unpack():
    src = randomtable(2, 10)
    tbl = UnpackView(src, 'f1', newfields=['foo', 'bar'])
    before = tbl.cachetag()
    src.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_join():
    srca = randomtable(2, 10)
    srcb = randomtable(2, 10)
    tbl = JoinView(srca, srcb, 'f0')
    before = tbl.cachetag()
    srca.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_implicitjoin():
    srca = randomtable(2, 10)
    srcb = randomtable(4, 10)
    tbl = ImplicitJoinView(srca, srcb)
    before = tbl.cachetag()
    srca.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_crossjoin():
    srca = randomtable(2, 10)
    srcb = randomtable(4, 10)
    tbl = CrossJoinView(srca, srcb)
    before = tbl.cachetag()
    srca.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_antijoin():
    srca = randomtable(2, 10)
    srcb = randomtable(4, 10)
    tbl = AntiJoinView(srca, srcb, 'f0')
    before = tbl.cachetag()
    srca.reseed()
    after = tbl.cachetag()
    assert before != after
    
def test_implicitantijoin():
    srca = randomtable(2, 10)
    srcb = randomtable(4, 10)
    tbl = ImplicitAntiJoinView(srca, srcb)
    before = tbl.cachetag()
    srca.reseed()
    after = tbl.cachetag()
    assert before != after
    

 
    


    