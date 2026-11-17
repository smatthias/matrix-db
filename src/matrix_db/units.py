"""Small explicit registry of affine conversions; unsupported conversions fail closed."""
import numpy as np

# quantity group, multiplier and offset to the group's reference unit.
UNITS={
    'K':('temperature',1.,0.),'degC':('temperature',1.,273.15),'degF':('temperature',5/9,273.15-32*5/9),
    'delta_K':('temperature_difference',1.,0.),'delta_degC':('temperature_difference',1.,0.),
    'delta_degF':('temperature_difference',5/9,0.),
    'm':('length',1.,0.),'mm':('length',0.001,0.),'cm':('length',0.01,0.),'km':('length',1000.,0.),
    's':('time',1.,0.),'min':('time',60.,0.),'h':('time',3600.,0.),
    'm/s':('speed',1.,0.),'km/h':('speed',1/3.6,0.),'kn':('speed',1852/3600,0.),
    'Pa':('pressure',1.,0.),'hPa':('pressure',100.,0.),'kPa':('pressure',1000.,0.),
    '1':('ratio',1.,0.),'%':('ratio',0.01,0.),
    'mm/h':('precipitation_rate',1.,0.),'mm/s':('precipitation_rate',3600.,0.),
    'kg/m3':('density',1.,0.),'g/m3':('density',0.001,0.),
}

def conversion(source,target):
    if source==target:return 1.,0.
    if source not in UNITS or target not in UNITS:raise ValueError('unsupported unit conversion')
    group,scale,offset=UNITS[source];target_group,target_scale,target_offset=UNITS[target]
    if group!=target_group:raise ValueError('incompatible unit quantities')
    return scale/target_scale,(offset-target_offset)/target_scale

def convert(values,source,target):
    scale,offset=conversion(source,target)
    return np.asarray(values,dtype=np.float64)*scale+offset
