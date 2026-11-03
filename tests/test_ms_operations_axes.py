"""mstools operations act on the right axis of (nrow, nchan, ncorr) columns, once per row."""
import numpy as np
from casacore import tables as pt
from evn_postprocess.mstools import operations


def make_ms(path, ants=('EF', 'WB', 'JB')):
    """Rows: EF-EF (auto), EF-WB, WB-JB; DATA[row, chan, corr] = 10*chan + corr."""
    desc = pt.maketabdesc([pt.makescacoldesc('ANTENNA1', 0), pt.makescacoldesc('ANTENNA2', 0),
                           pt.makescacoldesc('TIME', 0.0),
                           pt.makearrcoldesc('DATA', 0j, ndim=2, shape=[3, 2])])
    data = np.array([[[10 * c + p for p in range(2)] for c in range(3)]] * 3, dtype=complex)
    with pt.table(str(path), desc, nrow=3, ack=False) as t:
        t.putcol('ANTENNA1', np.array([0, 0, 1]))
        t.putcol('ANTENNA2', np.array([0, 1, 2]))
        t.putcol('TIME', np.full(3, 5e9))
        t.putcol('DATA', data)
    with pt.table(str(path / 'ANTENNA'), pt.maketabdesc([pt.makescacoldesc('NAME', '')]), nrow=len(ants),
                  ack=False) as a:
        a.putcol('NAME', list(ants))
    with pt.table(str(path / 'OBSERVATION'), pt.maketabdesc([pt.makearrcoldesc('TIME_RANGE', 0.0, ndim=1,
                                                                                shape=[2])]), nrow=1, ack=False) as o:
        o.putcol('TIME_RANGE', np.array([[4.9e9, 5.1e9]]))
    with pt.table(str(path), readonly=False, ack=False) as t:
        t.putkeyword('ANTENNA', 'Table: ' + str(path / 'ANTENNA'))
        t.putkeyword('OBSERVATION', 'Table: ' + str(path / 'OBSERVATION'))
    return data


def test_invert_subband_reverses_channels_once(tmp_path):
    ms = tmp_path / "x.ms"
    original = make_ms(ms)
    operations.invert_subband(ms, 'Ef')
    with pt.table(str(ms), ack=False) as t:
        data = t.getcol('DATA')
    assert np.array_equal(data[0], original[0][::-1, :])   # autocorrelation: flipped exactly once
    assert np.array_equal(data[1], original[1][::-1, :])   # EF-WB: channels reversed, pols kept
    assert np.array_equal(data[2], original[2])            # WB-JB: untouched
