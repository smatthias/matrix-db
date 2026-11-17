"""Explicit, bounded scalar-field alignment on regular grids."""
from datetime import datetime
import hashlib
import itertools
import math
from typing import Annotated, Literal
from uuid import UUID
import numpy as np
import pyproj
from pydantic import Field, model_validator
from .arrays import Axis, ArrayStore, GridSpec, SubsetQuery, MAX_BYTES, canonical
from .catalogue import TimeCoverage
from .contracts import Contract, Identifier
from .units import conversion

ALGORITHM_VERSION='matrix-db-alignment-1'

class AlignmentInput(Contract):
    version_id: UUID
    variable_id: Identifier
    output_variable: Identifier
    categorical: bool=False
    spatial: Literal['nearest','linear','conservative']='nearest'
    spatial_extensive: bool=False
    temporal: Literal['exact','nearest','linear','overlap']='exact'
    time_semantics: Literal['instantaneous','interval_mean','accumulation']='instantaneous'
    source_time_bounds: list[TimeCoverage] | None=None
    target_time_bounds: list[TimeCoverage] | None=None
    vertical: Literal['exact','nearest','linear']='exact'
    vertical_offset_metres: float | None=None
    max_time_gap_seconds: float | None=Field(default=None,ge=0)
    max_vertical_gap_metres: float | None=Field(default=None,ge=0)
    max_horizontal_gap: float | None=Field(default=None,ge=0)
    minimum_coverage: float=Field(default=1.,gt=0,le=1)
    renormalize_missing: bool=False
    broadcast: list[Literal['time','vertical']]=Field(default_factory=list)

    @model_validator(mode='after')
    def explicit_semantics(self):
        if self.categorical and (self.spatial!='nearest' or self.temporal not in ('exact','nearest') or self.vertical not in ('exact','nearest')):
            raise ValueError('categories require nearest/exact selection')
        if self.spatial_extensive and self.spatial!='conservative':raise ValueError('cell totals require conservative spatial remapping')
        if self.temporal in ('nearest','linear') and self.max_time_gap_seconds is None:
            raise ValueError('temporal interpolation requires an explicit maximum gap')
        if self.vertical in ('nearest','linear') and self.max_vertical_gap_metres is None:
            raise ValueError('vertical interpolation requires an explicit maximum gap')
        if self.time_semantics!='instantaneous' and self.temporal!='overlap':
            raise ValueError('interval means and accumulations require overlap alignment')
        if self.temporal=='overlap' and (self.time_semantics=='instantaneous' or self.source_time_bounds is None or self.target_time_bounds is None):
            raise ValueError('overlap requires interval semantics and explicit source/target bounds')
        if self.temporal!='overlap' and (self.source_time_bounds is not None or self.target_time_bounds is not None):
            raise ValueError('time bounds require overlap alignment')
        if self.renormalize_missing and (self.spatial_extensive or self.time_semantics=='accumulation'):
            raise ValueError('renormalizing missing data would invent extensive totals')
        return self

class AlignmentRequest(Contract):
    target: GridSpec
    inputs: list[AlignmentInput]=Field(min_length=1,max_length=16)

    @model_validator(mode='after')
    def outputs_match(self):
        outputs=[source.output_variable for source in self.inputs]
        if len(set(outputs))!=len(outputs) or set(outputs)!={v.id for v in self.target.variables}:
            raise ValueError('each target variable requires exactly one input')
        return self


def role(axis):
    return {'longitude':'horizontal_x','x':'horizontal_x','latitude':'horizontal_y','y':'horizontal_y'}.get(axis.kind,
        axis.kind if axis.kind!='other' else f'other:{axis.name}')

def coordinates(axis):
    if axis.kind=='time':return np.array([datetime.fromisoformat(v).timestamp() for v in axis.values])
    return np.array(axis.values,dtype=float)


def point_weights(axis,value,method,max_gap=None):
    """Return (source index, value weight, covered fraction). No centre extrapolation."""
    values=coordinates(axis);order=np.argsort(values);ordered=values[order]
    exact=np.flatnonzero(np.isclose(ordered,value,rtol=0,atol=1e-8 if axis.kind=='time' else 1e-10))
    if len(exact):return [(int(order[exact[0]]),1.,1.)]
    if method=='exact':return []
    if method=='nearest':
        if axis.bounds is not None:
            if not any(low<=value<=high for low,high in axis.bounds):return []
        elif value<ordered[0] or value>ordered[-1]:return []
        index=int(np.argmin(np.abs(ordered-value)))
        if max_gap is not None and abs(ordered[index]-value)>max_gap:return []
        return [(int(order[index]),1.,1.)]
    if method!='linear':raise ValueError('unsupported point alignment method')
    high=int(np.searchsorted(ordered,value))
    if high==0 or high==len(ordered):return []
    low=high-1;gap=ordered[high]-ordered[low]
    if max_gap is not None and gap>max_gap:return []
    fraction=(value-ordered[low])/gap
    return [(int(order[low]),1-fraction,1-fraction),(int(order[high]),fraction,fraction)]


def overlap_weights(source_bounds,target_bounds,*,extensive=False,metric=lambda v:v):
    low,high=map(metric,target_bounds)
    if not low<high:raise ValueError('target interval must have positive extent')
    result=[]
    for index,(left,right) in enumerate(source_bounds):
        left,right=metric(left),metric(right)
        overlap=max(0.,min(high,right)-max(low,left))
        if overlap:
            result.append((index,overlap/(right-left if extensive else high-low),overlap/(high-low)))
    return result


def time_bounds(bounds,axis):
    if len(bounds)!=len(axis.values):raise ValueError('time bounds must match axis length')
    result=[]
    for window,value in zip(bounds,coordinates(axis)):
        low,high=window.start.timestamp(),window.end.timestamp()
        if not low<high or not low<=value<=high:raise ValueError('time bounds must enclose coordinates with positive duration')
        if result and result[-1][1]>low:raise ValueError('source/target time intervals must not overlap')
        result.append((low,high))
    return result


def cast_output(data,mask,dtype):
    dtype=np.dtype(dtype)
    valid=data[~mask]
    if not np.isfinite(valid).all():raise ValueError('alignment produced nonfinite values')
    if dtype.kind in ('i','u'):
        info=np.iinfo(dtype)
        if np.any(valid<info.min) or np.any(valid>info.max) or np.any(valid!=np.floor(valid)):
            raise ValueError('alignment cannot represent values exactly in target integer dtype')
    with np.errstate(over='ignore',invalid='ignore'):
        result=np.where(mask,0,data).astype(dtype)
    if dtype.kind=='f' and not np.isfinite(result[~mask]).all():raise ValueError('target dtype overflows')
    return result


def align_field(data,mask,source_spec,source_variable,target_spec,target_variable,options):
    source_axes={a.name:a for a in source_spec.axes};target_axes={a.name:a for a in target_spec.axes}
    ordered_source=[source_axes[name] for name in source_variable.dimensions]
    ordered_target=[target_axes[name] for name in target_variable.dimensions]
    source_roles={role(axis):axis for axis in ordered_source};target_roles={role(axis):axis for axis in ordered_target}
    if set(source_roles)-set(target_roles):raise ValueError('target cannot drop source dimensions during alignment')
    extra=set(target_roles)-set(source_roles)
    if not extra.issubset(set(options.broadcast)):raise ValueError('missing source dimensions require explicit broadcasting')
    if any(key not in extra for key in options.broadcast):raise ValueError('broadcast names must identify absent source dimensions')
    if options.temporal=='overlap' and 'time' not in source_roles:raise ValueError('interval alignment requires a time dimension')
    scale,offset=conversion(source_variable.unit,target_variable.unit)
    if options.categorical and source_variable.unit!=target_variable.unit:raise ValueError('category codes cannot change units')
    if offset and (options.spatial_extensive or options.time_semantics=='accumulation'):
        raise ValueError('offset unit conversion is undefined for extensive totals')
    if options.spatial=='conservative' and source_spec.crs!=target_spec.crs:
        raise ValueError('conservative alignment currently requires matching CRS')
    if 'vertical' in source_roles:
        source_vertical=source_roles['vertical'];target_vertical=target_roles['vertical']
        if source_vertical.reference!=target_vertical.reference and options.vertical_offset_metres is None:
            raise ValueError('different vertical references require an explicit constant offset')
    for key,axis in source_roles.items():
        if key.startswith('other:') and axis.values!=target_roles[key].values:
            raise ValueError('other dimensions must match exactly')
    horizontal_cells=len(target_roles['horizontal_x'].values)*len(target_roles['horizontal_y'].values)
    comparisons=horizontal_cells*(len(source_roles['horizontal_x'].values)+len(source_roles['horizontal_y'].values))
    comparisons+=sum(len(axis.values)*len(target_roles[key].values) for key,axis in source_roles.items() if not key.startswith('horizontal_'))
    if comparisons>5000000:raise ValueError('alignment coordinate work exceeds budget; split the target grid')
    expected=tuple(len(axis.values) for axis in ordered_source)
    if data.shape!=expected or mask.shape!=expected or mask.dtype!=bool:raise ValueError('source shape/mask mismatch')
    if data.dtype.kind not in ('i','u','f') or not np.isfinite(data[~mask]).all():raise ValueError('invalid source payload')
    # Float64 computations are explicit; reject integers that would lose identity/precision.
    if data.dtype.kind in ('i','u') and np.any(np.abs(data[~mask].astype(object))>2**53):
        raise ValueError('alignment float64 arithmetic cannot exactly represent these integers')
    shape=tuple(len(axis.values) for axis in ordered_target)
    if math.prod(shape)*17>MAX_BYTES:raise ValueError('aligned data/mask/coverage exceeds 64 MiB')
    if math.prod(shape)>100000:raise ValueError('alignment exceeds 100000 output cells; request a smaller grid')
    output=np.zeros(shape,dtype='float64');missing=np.ones(shape,dtype=bool);coverage=np.zeros(shape,dtype='float64')
    source_intervals=target_intervals=None
    if options.temporal=='overlap':
        source_intervals=time_bounds(options.source_time_bounds,source_roles['time'])
        target_intervals=time_bounds(options.target_time_bounds,target_roles['time'])
    transform=None
    if source_spec.crs!=target_spec.crs:
        transform=pyproj.Transformer.from_crs(target_spec.crs,source_spec.crs,always_xy=True)
    spatial_cache={};axis_cache={};evaluations=0
    for destination in np.ndindex(shape):
        positions={role(axis):index for axis,index in zip(ordered_target,destination)}
        horizontal=(positions['horizontal_x'],positions['horizontal_y'])
        if horizontal not in spatial_cache:
            x=target_roles['horizontal_x'].values[horizontal[0]];y=target_roles['horizontal_y'].values[horizontal[1]]
            if transform is not None:x,y=transform.transform(x,y,errcheck=True)
            weights={}
            for key,value in [('horizontal_x',x),('horizontal_y',y)]:
                axis=source_roles[key]
                if options.spatial=='conservative':
                    target_axis=target_roles[key]
                    metric=math.radians if axis.kind=='longitude' else (lambda v:math.sin(math.radians(v))) if axis.kind=='latitude' else lambda v:v
                    weights[key]=overlap_weights(axis.bounds,target_axis.bounds[positions[key]],extensive=options.spatial_extensive,metric=metric)
                else:weights[key]=point_weights(axis,value,options.spatial,options.max_horizontal_gap)
            spatial_cache[horizontal]=weights
        contributors=[]
        for axis in ordered_source:
            key=role(axis);target_axis=target_roles[key];index=positions[key]
            if key.startswith('horizontal_'):weights=spatial_cache[horizontal][key]
            elif (key,index) in axis_cache:weights=axis_cache[(key,index)]
            elif key=='time':
                if options.temporal=='overlap':
                    weights=overlap_weights(source_intervals,target_intervals[index],extensive=options.time_semantics=='accumulation')
                else:weights=point_weights(axis,coordinates(target_axis)[index],options.temporal,options.max_time_gap_seconds)
            elif key=='vertical':
                target_value=target_axis.values[index]*(1 if target_axis.positive=='up' else -1)
                source_value=(target_value+(options.vertical_offset_metres or 0))*(1 if axis.positive=='up' else -1)
                weights=point_weights(axis,source_value,options.vertical,options.max_vertical_gap_metres)
            else:weights=[(index,1.,1.)]
            if not key.startswith('horizontal_'):axis_cache[(key,index)]=weights
            contributors.append(weights)
        count=math.prod(len(weights) for weights in contributors)
        evaluations+=count
        if evaluations>2000000:raise ValueError('alignment exceeds 2000000 contributions; split the request')
        value=covered=0.
        for combination in itertools.product(*contributors):
            source_index=tuple(item[0] for item in combination)
            if mask[source_index]:continue
            value+=float(data[source_index]*scale+offset)*math.prod(item[1] for item in combination)
            covered+=math.prod(item[2] for item in combination)
        coverage[destination]=min(1.,max(0.,covered))
        if covered>0 and covered+1e-10>=options.minimum_coverage:
            output[destination]=value/covered if options.renormalize_missing else value
            missing[destination]=False
    return {'data':cast_output(output,missing,target_variable.dtype),'mask':missing,'coverage':coverage,
            'dimensions':target_variable.dimensions,'unit':target_variable.unit}


def align(arrays:ArrayStore,request:AlignmentRequest):
    request=AlignmentRequest.model_validate_json(request.model_dump_json())
    target_variables={v.id:v for v in request.target.variables};target_axes={a.name:a for a in request.target.axes}
    total=sum(math.prod(len(target_axes[name].values) for name in v.dimensions)*17 for v in request.target.variables)
    if total>MAX_BYTES:raise ValueError('combined aligned output exceeds 64 MiB')
    result={};sources=[]
    for source in request.inputs:
        manifest=arrays.manifest(source.version_id);spec=GridSpec.model_validate(manifest['spec'])
        variable=next((v for v in spec.variables if v.id==source.variable_id),None)
        if variable is None:raise ValueError('unknown source variable')
        input_axes={a.name:a for a in spec.axes}
        if math.prod(len(input_axes[name].values) for name in variable.dimensions)*9>MAX_BYTES:
            raise ValueError('source exceeds alignment working budget; stage a smaller source version')
        values=arrays.read(SubsetQuery(version_id=source.version_id,variables=[source.variable_id]))
        field=values['variables'][source.variable_id]
        if field.get('coverage') is not None and np.any((field['coverage']<1-1e-10)&~field['mask']):
            raise ValueError('chained alignment requires full coverage in nonmissing source cells')
        result[source.output_variable]=align_field(field['data'],field['mask'],spec,variable,request.target,
                                                   target_variables[source.output_variable],source)
        sources.append({'version_id':str(source.version_id),'manifest_checksum':hashlib.sha256(canonical(manifest).encode()).hexdigest(),
                        'parameters':source.model_dump(mode='json'),'source_unit':variable.unit,
                        'unit_conversion':dict(zip(('scale','offset'),conversion(variable.unit,target_variables[source.output_variable].unit)))})
    provenance={'algorithm':ALGORITHM_VERSION,'numpy':np.__version__,'pyproj':pyproj.__version__,
                'proj':pyproj.proj_version_str,'unit_registry':1,'target':request.target.model_dump(mode='json'),'sources':sources}
    return {'variables':result,'target':request.target.model_dump(mode='json'),'provenance':provenance}


def align_and_publish(arrays,request):
    result=align(arrays,request)
    def provider(variable,slices):
        field=result['variables'][variable]
        return field['data'][slices],field['mask'][slices],field['coverage'][slices]
    version=arrays.publish(request.target,provider,provenance=result['provenance'])
    return {'version_id':str(version),'provenance':result['provenance']}
