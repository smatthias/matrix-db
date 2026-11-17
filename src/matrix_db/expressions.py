"""Bounded expression AST interpretation with dimensional checks. Never uses eval."""
import ast
from dataclasses import dataclass
import numpy as np
from .units import UNITS

# Length, time, temperature, mass exponents in SI units.
DIMENSIONS={'length':(1,0,0,0),'time':(0,1,0,0),'speed':(1,-1,0,0),
    'pressure':(-1,-2,0,1),'ratio':(0,0,0,0),'temperature':(0,0,1,0),
    'temperature_difference':(0,0,1,0),'density':(-3,0,0,1),'precipitation_rate':(1,-1,0,0)}

@dataclass(frozen=True)
class Signature:
    dimensions: tuple
    temperature: str='none'

RATIO=Signature((0,0,0,0))


def unit_info(unit):
    if unit not in UNITS:raise ValueError(f'unsupported calculation unit: {unit}')
    group,scale,offset=UNITS[unit]
    if group=='precipitation_rate':scale*=.001/3600;offset*=.001/3600
    temperature='absolute' if group=='temperature' else 'difference' if group=='temperature_difference' else 'none'
    return Signature(DIMENSIONS[group],temperature),scale,offset


def parse(expression):
    if len(expression)>2048:raise ValueError('expression exceeds 2048 characters')
    try:tree=ast.parse(expression,mode='eval')
    except (SyntaxError,RecursionError) as error:raise ValueError('invalid expression syntax') from error
    if len(list(ast.walk(tree)))>128:raise ValueError('expression exceeds 128 AST nodes')
    return tree.body


def binary_signature(operator,left,right):
    if isinstance(operator,(ast.Add,ast.Sub)):
        if left.dimensions!=right.dimensions:raise ValueError('addition/subtraction requires matching dimensions')
        if left.temperature=='absolute' and right.temperature=='absolute':
            if isinstance(operator,ast.Add):raise ValueError('use the registered mean function to average absolute temperatures')
            return Signature(left.dimensions,'difference')
        if left.temperature=='difference' and right.temperature=='absolute' and isinstance(operator,ast.Sub):
            raise ValueError('cannot subtract absolute temperature from a temperature difference')
        return Signature(left.dimensions,'absolute' if 'absolute' in (left.temperature,right.temperature) else left.temperature)
    if isinstance(operator,(ast.Mult,ast.Div)):
        if 'absolute' in (left.temperature,right.temperature):raise ValueError('absolute temperatures cannot be multiplied/divided in expressions')
        sign=1 if isinstance(operator,ast.Mult) else -1
        dimensions=tuple(a+sign*b for a,b in zip(left.dimensions,right.dimensions))
        return Signature(dimensions,'difference' if dimensions==DIMENSIONS['temperature_difference'] else 'none')
    raise ValueError('unsupported arithmetic operator')


def analyze(node,names):
    if isinstance(node,ast.Name):
        if node.id not in names:raise ValueError(f'unknown expression name: {node.id}')
        return names[node.id],{node.id}
    if isinstance(node,ast.Constant):
        if type(node.value) not in (int,float) or abs(node.value)>1e12 or not np.isfinite(node.value):
            raise ValueError('only finite numeric literals up to 1e12 are supported')
        return RATIO,set()
    if isinstance(node,ast.UnaryOp) and isinstance(node.op,(ast.UAdd,ast.USub)):
        signature,used=analyze(node.operand,names)
        if signature.temperature=='absolute':raise ValueError('absolute temperature sign changes are unsupported')
        return signature,used
    if isinstance(node,ast.BinOp):
        left,used_left=analyze(node.left,names)
        if isinstance(node.op,ast.Pow):
            exponent=node.right.value if isinstance(node.right,ast.Constant) else (-node.right.operand.value if isinstance(node.right,ast.UnaryOp) and isinstance(node.right.op,ast.USub) and isinstance(node.right.operand,ast.Constant) and type(node.right.operand.value) is int else None)
            if type(exponent) is not int or not -4<=exponent<=4:
                raise ValueError('powers require an integer literal between -4 and 4')
            if left.temperature=='absolute':raise ValueError('absolute temperature powers are unsupported')
            power=exponent
            dimensions=tuple(value*power for value in left.dimensions)
            return Signature(dimensions,'difference' if dimensions==DIMENSIONS['temperature_difference'] else 'none'),used_left
        right,used_right=analyze(node.right,names)
        return binary_signature(node.op,left,right),used_left|used_right
    if isinstance(node,ast.Compare):
        if len(node.ops)!=1 or not isinstance(node.ops[0],(ast.Gt,ast.GtE,ast.Lt,ast.LtE,ast.Eq,ast.NotEq)):
            raise ValueError('only single numeric comparisons are supported')
        left,used_left=analyze(node.left,names);right,used_right=analyze(node.comparators[0],names)
        if left!=right:raise ValueError('comparisons require compatible quantity dimensions')
        return RATIO,used_left|used_right
    if isinstance(node,ast.Call) and isinstance(node.func,ast.Name) and not node.keywords:
        signatures=[];used=set()
        for argument in node.args:
            signature,names_used=analyze(argument,names);signatures.append(signature);used|=names_used
        function=node.func.id
        if function in ('sqrt','abs') and len(signatures)==1:
            signature=signatures[0]
            if signature.temperature=='absolute':raise ValueError('unsupported function on absolute temperature')
            if function=='abs':return signature,used
            if any(exponent%2 for exponent in signature.dimensions):raise ValueError('sqrt requires even dimension exponents')
            dimensions=tuple(exponent//2 for exponent in signature.dimensions)
            return Signature(dimensions,'difference' if dimensions==DIMENSIONS['temperature_difference'] else 'none'),used
        if function in ('hypot','minimum','maximum') and len(signatures)==2 and signatures[0]==signatures[1]:
            if function=='hypot' and signatures[0].temperature=='absolute':raise ValueError('hypot requires non-absolute quantities')
            return signatures[0],used
        raise ValueError('unsupported expression function or argument dimensions')
    raise ValueError('unsupported expression syntax; attributes, indexing and arbitrary calls are forbidden')


def evaluate(node,values):
    if isinstance(node,ast.Name):return values[node.id]
    if isinstance(node,ast.Constant):return float(node.value)
    if isinstance(node,ast.UnaryOp):
        value=evaluate(node.operand,values)
        return -value if isinstance(node.op,ast.USub) else value
    if isinstance(node,ast.BinOp):
        functions={ast.Add:np.add,ast.Sub:np.subtract,ast.Mult:np.multiply,ast.Div:np.divide,ast.Pow:np.power}
        return functions[type(node.op)](evaluate(node.left,values),evaluate(node.right,values))
    if isinstance(node,ast.Compare):
        functions={ast.Gt:np.greater,ast.GtE:np.greater_equal,ast.Lt:np.less,ast.LtE:np.less_equal,ast.Eq:np.equal,ast.NotEq:np.not_equal}
        return functions[type(node.ops[0])](evaluate(node.left,values),evaluate(node.comparators[0],values))
    functions={'sqrt':np.sqrt,'abs':np.abs,'hypot':np.hypot,'minimum':np.minimum,'maximum':np.maximum}
    return functions[node.func.id](*[evaluate(argument,values) for argument in node.args])


def registered_signature(function,signatures):
    required={'wind_speed':{'east','north'},'threshold_exceedance':{'value','threshold'},'difference':{'left','right'},'mean':{'left','right'}}
    if function not in required or set(signatures)!=required[function]:raise ValueError('registered function arguments do not match its signature')
    left,right=[signatures[name] for name in sorted(signatures)]
    if left!=right:raise ValueError('registered function inputs require matching quantity dimensions')
    if function=='wind_speed':
        if left.dimensions!=DIMENSIONS['speed']:raise ValueError('wind components must have speed dimensions')
        return left
    if function=='threshold_exceedance':return RATIO
    if function=='difference':return binary_signature(ast.Sub(),signatures['left'],signatures['right'])
    return left


def registered_evaluate(function,values):
    if function=='wind_speed':return np.hypot(values['east'],values['north'])
    if function=='threshold_exceedance':return np.greater(values['value'],values['threshold'])
    if function=='difference':return np.subtract(values['left'],values['right'])
    return (values['left']+values['right'])/2
