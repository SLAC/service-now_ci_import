#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import ASCENDING, DESCENDING

from copy import deepcopy

import os.path, time
from datetime import datetime, timedelta
from math import ceil

import unicodedata
import cPickle as pickle

from re import match, search, compile, sub, IGNORECASE
import os

import logging

from util import mac_address, parse_number
from connect import get_mongo_collection

CONTENT_REMAP = {
    'model': {
        
        'SUNFIRE V210': 'V210',
        'SUNFIRE V240': 'V240',
        'SUNFIRE V245': 'V245',
        
        'X2200 M2 WITH QUAD CORE PROCESSOR': 'X2200',
        'X2200 M2 WITH DUAL CORE PROCESSOR': 'X2200',
        'X2200 M2': 'X2200',
        'SUNFIRE X2200': 'X2200',
        'SUNFIRE X2270': 'X2270',
        'SUNFIRE X2270 M2': 'X2270',
        'X2270 M2': 'X2270',
        'FIRE X2270': 'X2270',

        'X4100 SERVER': 'X4100',
        'X4100 M2': 'X4100',
        'SUNFIRE X4100': 'X4100',
        'SUNFIRE X4140': 'X4140',
        'SUNFIRE 4150': 'X4150',
        'SUNFIRE X4150': 'X4150',
        'SUNFIRE X4170': 'X4170',
        'SUNFIRE X4540': 'X4540',

        'X4170 SERVER': 'X4170',
        'SUNFIRE X4200': 'X4200',
        'X4200 SERVER': 'X4200',
        'X4200 M2': 'X4200',

        'SUNFIRE X4240': 'X4240',

        'SUNFIRE X4250': 'X4250',
        'SUNFIRE X4270': 'X4270',
        'SUNFIRE X4275': 'X4275',
        'X4270 M2': 'X4270',
        'X4270 SERVER': 'X4270',
        'X4275 SERVER': 'X4275',

        'SUNFIRE V20Z': 'V20z',
        'V20Z': 'V20z',

        'PROLIANT DL360': 'ProLiant DL360',
        'DL380 G6 - LFF': 'ProLiant DL380 G6',
        'PROLIANT DL380': 'ProLiant DL380 G6',
        'PROLIANT DL380 G5': 'ProLiant DL380 G5',
        'PROLIANT DL380 G6': 'ProLiant DL380 G6',
        'PROLIANT DL380G6': 'ProLiant DL380 G6',
        'PROLIANT DL380 G': 'ProLiant DL380 G6',
        
        'PROLIANT-SL2X170': 'ProLiant SL2x170z G6',
        'PROLIANT SL2X170': 'ProLiant SL2x170z G6',
        'PROLIANT SL2X170 G6': 'ProLiant SL2x170z G6',
        'PROLIANT SL6000': 'ProLiant SL2x170z G6',
        'SL2X170Z G6': 'ProLiant SL2x170z G6',

        'POWEREDGE 1550': 'PowerEdge 1550',
        'POWEREDGE 1650': 'PowerEdge 1650',
        'POWEREDGE 1750': 'PowerEdge 1750',
        '1850': 'PowerEdge 1850',
        '1950': 'PowerEdge 1950',
        'POWEREDGE 1850': 'PowerEdge 1850',
        'POWEREDGE 1950': 'PowerEdge 1950',
        'POWEREDGE 1950 III': 'PowerEdge 1950',
        'POWEREDGE 2650': 'PowerEdge 2650',
        'POWEREDGE 2650 (OPEN VIEW)': 'PowerEdge 2650',
        'POWEREDGE 2850': 'PowerEdge 2850',
        'POWEREDGE 2950': 'PowerEdge 2950',
        'POWEREDGE 2950-III': 'PowerEdge 2950',
        '2950': 'PowerEdge 2950',
        'POWEREDGE R200': 'PowerEdge R200',
        'R210': 'PowerEdge R210',
        'POWEREDGE R310': 'PowerEdge R310',
        'POWEREDGE R410 - CLOSED VIEW': 'PowerEdge R410',
        'POWEREDGE R410 - LFF': 'PowerEdge R410',
        'POWEREDGE R420': 'PowerEdge R420',
        'POWEREDGE R510-8': 'PowerEdge R510',
        'POWEREDGE R520': 'PowerEdge R520',
        'POWEREDGE R410': 'PowerEdge R410',
        'R510': 'PowerEdge R510',
        'POWEREDGE R510-12': 'PowerEdge R510',
        # 'PowerEdge R515-12':
        'POWEREDGE R510-4': 'PowerEdge R510',
        'POWEREDGE R510': 'PowerEdge R510',
        'POWEREDGE R515-12': 'PowerEdge R515',
        'R610': 'PowerEdge R610',
        'POWEREDGE R610': 'PowerEdge R610',
        'POWEREDGE T610': 'PowerEdge T610',
        'POWEREDGE T610 - RACKMOUNT': 'PowerEdge T610',
        'R620': 'PowerEdge R620',
        'POWEREDGE R620 10-SFF': 'PowerEdge R620',
        'R710': 'PowerEdge R710',
        'POWEREDGE R710': 'PowerEdge R710',
        'POWEREDGE R715': 'PowerEdge R715',
        'POWEREDGE R720': 'PowerEdge R720',
        'POWEREDGE R720 - LFF': 'PowerEdge R720',
        'POWEREDGE R720 - SFF': 'PowerEdge R720',
        'POWEREDGE R720XD': 'PowerEdge R720xd',
        'POWEREDGE R720XD - LFF': 'PowerEdge R720xd',
        'POWEREDGE R720XD - SFF': 'PowerEdge R720xd',
        'POWEREDGE R810': 'PowerEdge R810',
        'POWEREDGE R820': 'PowerEdge R820',
        'POWEREDGE R900': 'PowerEdge R900',
        'R910': 'PowerEdge R910',
        'POWEREDGE R620 8-SFF': 'PowerEdge R620',
        'POWEREDGE R620': 'PowerEdge R620',
        'POWEREDGE M620': 'PowerEdge M620',
        'POWEREDGE M620': 'PowerEdge M620',
        'M620': 'PowerEdge M620',
        'POWEREDGE R910': 'PowerEdge R910',

        'POWEREDGE C6100': 'PowerEdge C6100',
        'C6100': 'PowerEdge C6100',
        'POWEREDGE C6100 - LFF': 'PowerEdge C6100',
        'POWEREDGE C6100 - SFF': 'PowerEdge C6100',

        'PRECISION WORKSTATION 340': 'Precision 340', 
        'PRECISION 360': 'Precision 360',
        'PRECISION 360N': 'Precision 360',
        'PRECISION WORKSTATION 360': 'Precision 360', 
        'PRECISION 370': 'Precision 370',
        'PRECISION 370N': 'Precision 370',
        'PRECISION WORKSTATION 370': 'Precision 370',
        'PRECISION WORKSTATION 380': 'Precision 380', 
        'PRECISION 380': 'Precision 380', 
        'PRECISION 380N': 'Precision 380', 
        'PRECISION 390': 'Precision 390', 
        'PRECISION 390N': 'Precision 390', 
        'PRECISION 410': 'Precision 410', 
        'PRECISION 410 MT': 'Precision 410', 
        'PRECISION 420': 'Precision 420', 
        'PRECISION 470': 'Precision 470', 
        'PRECISION WORKSTATION 390': 'Precision 390', 
        'PRECISION WORKSTATION 490': 'Precision 490',
        'PRECISION 490': 'Precision 490',
        'PRECISION 490N': 'Precision 490', 
        'PRECISION WORKSTATION 650': 'Precision 650', 
        'PRECISION 520': 'Precision 520',
        'PRECISION 530 MT': 'Precision 530',
        'PRECISION 610': 'Precision 610', 
        'PRECISION 620': 'Precision 620', 
        'PRECISION 650': 'Precision 650', 
        'PRECISION 650N': 'Precision 650', 
        'PRECISION 670N': 'Precision 670', 
        'PRECISION 690N': 'Precision 690', 
        'PRECISION 690': 'Precision 690', 

        'PRECISION T1700': 'Precision T1700',
        'PRECISION 3400': 'Precision T3400',
        'PRECISION T3400': 'Precision T3400',
        'PRECISION T3400N': 'Precision T3400',
        'PRECISION WORKSTATION T3400': 'Precision T3400', 

        'PRECISION T3500': 'Precision T3500',
        'PRECISION T3500N': 'Precision T3500',
        'PRECISION T3520N': 'Precision T3520',
        'PRECISION WORKSTATION T3500': 'Precision T3500', 

        'PRECISION T3600': 'Precision T3600',
        'PRECISION T3600N': 'Precision T3600',
        'PRECISION T3610': 'Precision T3610',
        'PRECISION T3610N': 'Precision T3610',

        'PRECISION M2400': 'Precision M2400',

        'DELL PRECISION M3800': 'Precision M3800',
        'PRECISION M3800': 'Precision M3800',
        
        'PRECISION M4400': 'Precision M4400',
        'PRECISION M4500': 'Precision M4500',
        'PRECISION M4600': 'Precision M4600',
        'PRECISION M4700': 'Precision M4700',
        'PRECISION M4800': 'Precision M4800',
        
        'PRECISION T5400': 'Precision T5400',
        'PRECISION T5400N': 'Precision T5400',
        'PRECISION WORKSTATION T5400': 'Precision T5400',
         
        'PRECISION T5500': 'Precision T5500',
        'PRECISION T5500E': 'Precision T5500',
        'PRECISION T5500N': 'Precision T5500',
        'PRECISION WORKSTATION T5500': 'Precision T5500', 
        'PRECISION T5600': 'Precision T5600',
        
        'PRECISION M90': 'Precision M90',

        'PRECISION M6400': 'Precision M6400',
        'PRECISION M6500': 'Precision M6500',
        'PRECISION M6600': 'Precision M6600',
        
        'PRECISION 670': 'Precision 670',
        'PRECISION 670N': 'Precision 670',
        'PRECISION WORKSTATION 670': 'Precision 670',
        'PRECISION WORKSTATION 690': 'Precision 690',
        'PRECISION 690N': 'Precision 690',
        
        'PRECISION T7400': 'Precision T7400',
        'PRECISION T7400N': 'Precision T7400',
        'PRECISION WORKSTATION T7400': 'Precision T7400',
        'PRECISION T7500': 'Precision T7500',
        'PRECISION WORKSTATION T7500': 'Precision T7500',
        'PRECISION T7500N': 'Precision T7500',
        'PRECISION T7600': 'Precision T7600',
        'PRECISION T7600N': 'Precision T7600',
        'PRECISION T7610': 'Precision T7610',
        
        'OPTIPLEX 380': 'OptiPlex 380',
        'OPTIPLEX 390': 'OptiPlex 390',

        'OPTIPLEX 740': 'OptiPlex 740',

        'OPTIPLEX 745': 'OptiPlex 745',
        'OPTIPLEX 755': 'OptiPlex 755',
        
        'OPTIPLEX 760': 'OptiPlex 760',
        'OPTIPLEX 760 SFF': 'OptiPlex 760',
        'OPTIPLEX 780': 'OptiPlex 780',
        'OPTIPLEX 960': 'OptiPlex 960',
        'OPTIPLEX 960MT': 'OptiPlex 960',
        'OPTIPLEX 980': 'OptiPlex 980',
        'OPTIPLEX 990': 'OptiPlex 990',
        'OPTIPLEX 990MT': 'OptiPlex 990',
        'OPTIPLEX 990DT': 'OptiPlex 990',
        'OPTIPLEX 9020': 'OptiPlex 9020',

        'OPTIPLEX 9010 USFF': 'OptiPlex 9010',
        'OPTIPLEX 9010': 'OptiPlex 9010',

        'GX150': 'OptiPlex GX150',
        'OPTIPLEX GX150': 'OptiPlex GX150',
        'OPTIPLEX GX240': 'OptiPlex GX240',
        'GX260': 'OptiPlex GX260',
        'OPTIPLEX GX260': 'OptiPlex GX260',
        'OPTIPLEX GX260T': 'OptiPlex GX260',

        'GX270': 'OptiPlex GX270',
        'OPTIPLEX GX270': 'OptiPlex GX270',
        'OPTIPLEX GX270 MT': 'OptiPlex GX270',
        'OPTIPLEX GX270T': 'OptiPlex GX270',
        'GX280': 'OptiPlex GX280',
        'GX300': 'OptiPlex GX300',
        'GX400': 'OptiPlex GX400',
        'OPTIPLEX GX400': 'OptiPlex GX400',
        'GX520': 'OptiPlex GX520',
        'GX620': 'OptiPlex GX620',

        'OPTIPLEX GX280': 'OptiPlex GX280',
        'OPTIPLEX SX280': 'OptiPlex SX280',
        'OPTIPLEX GX520': 'OptiPlex GX520',
        'OPTIPLEX GX620': 'OptiPlex GX620',
        'GX745': 'OptiPlex GX745',
        'GX755': 'OptiPlex GX755',

        'OPTIPLEX 745': 'OptiPlex GX745',
        'OPTIPLEX GX745': 'OptiPlex GX745',
        'OPTIPLEX 3020': 'OptiPlex 3020',
        'OPTIPLEX 7010': 'OptiPlex 7010',
        'OPTIPLEX 9010': 'OptiPlex 9010',
        'OPTIPLEX 9010 USFF': 'OptiPlex 9010',
        'OPTIPLEX 9020': 'OptiPlex 9020',
        
        'C800 LATITUDE': 'Latitude C800',
        
        'LATITUDE D620': 'Latitude D620',
        'LATITUDE D630': 'Latitude D630',
        'LATITUDE D430': 'Latitude D430',
        'LATITUDE D800': 'Latitude D800',
        'LATITUDE D820': 'Latitude D820',
        'LATITUDE D810': 'Latitude D810',
        'LATITUDE D830': 'Latitude D830',
        
        'LATITUDE E4200': 'Latitude E4200',
        'LATITUDE E4310': 'Latitude E4310',

        'LATITUDE E6230': 'Latitude E6230',
        'LATITUDE E6320': 'Latitude E6320',
        'LATITUDE E6330': 'Latitude E6330',
        'LATITUDE E6430S': 'Latitude E6430',
        'LATITUDE E6510': 'Latitude E6510',
        'LATITUDE E6530': 'Latitude E6530',
        'LATITUDE E6540': 'Latitude E6540',
        
        'LATITUDE E7240': 'Latitude E7240',
        'E7240-i7': 'Latitude E7240',
        'XT3': 'Latitude XT3',
        
        'LATITUDE E4300': 'Latitude E4300',
        'LATITUDE E5400': 'Latitude E5400',
        'LATITUDE E5500': 'Latitude E5500',

        'LATITUDE E6400': 'Latitude E6400',
        'LATITUDE E6410': 'Latitude E6410',
        'LATITUDE E6420': 'Latitude E6420',
        'LATITUDE E6420 ATG': 'Latitude E6420',
        'LATITUDE E6430': 'Latitude E6430',
        'CBX3___': 'Latitude E6430',
        'LATITUDE E6440': 'Latitude E6440',
        'LATITUDE 6400ATG': 'Latitude E6400',
        'LATITUDE E6500': 'Latitude E6500',
        'LATITUDE E6520': 'Latitude E6520',
        'LATITUDE E7440': 'Latitude E7440',
        
        'INSPIRON XPS-13': 'XPS L321X',
        'DELL SYSTEM XPS L321X': 'XPS L321X',
        'DIMENSION XPS 13': 'XPS L321X',
        'DIMENSION XPS-13': 'XPS L321X',
        'DIMENSION XPS L321X': 'XPS L321X',
        'XPSPRO': 'XPS L321X',
        'XPS13 9333': 'XPS L322X',

        'DELL SYSTEM XPS L322X': 'XPS L322X',
        'ULTRABOOK XPS-13': 'XPS L322X',
        'ULTRABOOK XPSL322X-13': 'XPS L322X',
        'XPS13': 'XPS L322X',
        'XPS L322X': 'XPS L322X',
        'XPS L421X': 'XPS L421X',
                
        'DIMENSION XPS-14': 'XPS L421X',
        'ULTRABOOK XPS-12': 'XPS 12 9Q23',
        'XPS 12 9Q23': 'XPS 12 9Q23',
        'XPS-15': 'XPS L521X',
        'XPS 15': 'XPS L521X',
        'DIMENSION XPS 15': 'XPS L521X',
        'XPS 15 9530': 'XPS L521X',
        
        'SURFACE WITH WINDOWS 8 PRO': 'Surface',
        'SURFACE PRO125GB': 'Surface',
        'SURFACE PRO 125GB': 'Surface',
        'SURFACE': 'Surface',
        'SURFACE PRO3': 'Surface Pro 3',
        'SURFACE PRO 3': 'Surface Pro 3',
        
        'MACBOOKAIR5,2': 'Macbook Air 13"',
        'MACBOOK AIR 13': 'Macbook Air 13"',
        'MACBOOK AIR 13"': 'Macbook Air 13"',
        'MACBOOKAIR4,2': 'Macbook Air 13"',
        'MACBOOK AIR': 'Macbook Air',
        'MACBOOK AIR 11""': 'Macbook Air 11"',
        'MACBOOK PRO 13"': 'Macbook Pro 13"',
        'MACBOOKPRO9,2': 'Macbook Pro 13"',
        'MACBOOK PRO 15': 'Macbook Pro 15"',
        'MACBOOK PRO15"': 'Macbook Pro 15"',
        'MACBOOK PRO 15"': 'Macbook Pro 15"',
        'MACBOOKPRO6,2': 'Macbook Pro 15"',
        'MACBOOKPRO10,2': 'Macbook Pro',
        'MACBOOKPRO5,3': 'Macbook Pro',
        'MACBOOK PRO': 'Macbook Pro',
        'MACBOOK PRO 13.3"': 'Macbook Pro 13"',
        'MACBOOK PRO13': 'Macbook Pro 13"',
        'MACBOOK PRO13"': 'Macbook Pro 13"',
        'MACBOOK PRO 17"': 'Macbook Pro 17"',
        'IMAC11,3': 'iMac 27"',
        'IMAC 20"': 'iMac 21"',
        'IMAC 21"': 'iMac 21"',
        'IMAC 21.5"': 'iMac 21"',
        'IMAC 24"': 'iMac 24"',
        'IMAC 27"': 'iMac 27"',
        'MAC PRO': 'Mac Pro',
        'MAC PRO CTO': 'Mac Pro',
        'MAC PRO XE5': 'Mac Pro',
        'A1186 MAC PRO': 'Mac Pro',
        
        'MAC II': 'Mac II',
        'MAC G3': 'Mac G3',
        'MAC G4': 'Mac G4',
        'MAC G5': 'Mac G5',

        'MACMINI4,1': 'Mac Mini',
        'MAC MINI': 'Mac Mini',
        
        'NEW IPAD AIR 16GB': 'iPAd Air',
                
        'VENUE 8 PRO 5830': 'Venue 8 Pro',
        'VENUE 8 PRO': 'Venue 8 Pro',
        'VENUE': 'Venue 11 Pro 5130',
        
        'ASPIRE': 'Aspire',
        
        'THINKPAD W520': 'Thinkpad W520',
        '4270CTO': 'Thinkpad W520',
        '2306CTO': 'Thinkpad 2360 CTO',
        'THINKPAD 2360 CTO': 'Thinkpad 2360 CTO',
        'THINKPAD 2306 CTO': 'Thinkpad 2360 CTO',
        '23539WU': 'Thinkpad T430S',
        'THINKPAD T430S': 'Thinkpad T430S',

        '33472YU': 'Thinkpad Twist',
        'THINKPAD TWIST': 'Thinkpad Twist',
        '20A7CTO1WW': 'Thinkpad X1 Carbon',
        '20A70037US': 'Thinkpad X1 Carbon',
        '3444F8U': 'Thinkpad X1 Carbon',
        'THINKPAD X1 CARBON': 'Thinkpad X1 Carbon',
        '367927U': 'Thinkpad Tablet 2',
        'THINKPAD TABLET 2 ': 'Thinkpad Tablet 2',
        'THINKPAD TABLET 2': 'Thinkpad Tablet 2',
        
        'KJ379AA-ABA A6400F': 'Pavilion 6400F',
        'A6400F PAVILION': 'Pavilion 6400F',
        
        'HP COMPAQ NX6325 (EN191UA#ABA)': 'NX6325',
        'NX6325': 'NX6325',
         
        'ASA 5220-AIP40-K9': 'ASA5520',
        'ASA 5520': 'ASA5520',
        
        'AM1640': 'Aspire M1640',
        'BIG IP': 'BIG-IP LTM 1600',
        'BIGIP LTM 1600': 'BIG-IP LTM 1600',
        'BIG-IP 1600': 'BIG-IP LTM 1600',
        
        'CATALYST 6509-NEB-A': 'WS-C6509-NEB-A',
        'WS-C6509-NEB-A': 'WS-C6509-NEB-A',
        'WS-C6509-NEBA-A': 'WS-C6509-NEB-A',
        'CATALYST 6509': 'WS-C6509',
        'CATALYST 6509-E': 'WS-C6509',
        'WS-C6509': 'WS-C6509',
        
        'CXT-8000': 'CXT8000',
        'N1912A': 'N1912A',
        
        'SILKWORM 4900': 'SilkWorm 4900',
        'SW4900': 'SilkWorm 4900',
        
        'SERVMAX TESLA GPU HPC': 'H8QG6',
        'STORAGETEK SL8500': 'Storagetek SL8500',
        'SL8500': 'Storagetek SL8500',
        
        
        'STOREDGE 3511': 'StorEdge 3511',
        '3511 FC': 'StorEdge 3511',
        
        'T3': 'StorEdge T3',
        'STOREDGE T3': 'StorEdge T3',
        '6120': 'StorEdge 6120',
        'STOREDGE 6120': 'StorEdge 6120',
        'T4': 'StorEdge 6120',
        'STOREDGE T4': 'StorEdge 6120',

        'ENTERPRISE T5120': 'Enterprise T5120',
        'T5120': 'Enterprise T5120',
        'ENTERPRISE T5140': 'Enterprise T5140',
        'T5140': 'Enterprise T5140',
        'SPARC STATION T5240': 'Sparc Station T5240',
        'T5240': 'Sparc Station T5240',
        'STOREDGE 6140': 'StorEdge 6140',

        'BROCADE 5100': '5100',
        
        
        
        'DESIGNJET4000': 'DESIGNJET 4000',
        
        'SYS-6016T-NTRF': '6016GT-TF-TM2',
        'SYS-6016GT-TF-FM205': 'X8DTG-D',
        
        # 'CAT 2900XL': 'Catalyst 2900XL',
        # 'CAT 2924XL': 'Catalyst 2924XL',
        # 'CAT 2950': 'Catalyst 2950',
        # 'CAT 2960 24': 'Catalyst 2960-24',
        # 'CAT 2960 48': 'Catalyst 2960-48',
        # 'CAT 2960 8': 'Catalyst 2960-8',
        # 'CAT 3524XL': 'Catalyst 3524XL',
        # 'CAT 3548XL': 'Catalyst 3548XL',
        # 'CAT 3550 24': 'Catalyst 3550-24',
        # 'CAT 3550 48': 'Catalyst 3550-48',
        # 'CAT 3560 24': 'Catalyst 3560-24',
        # 'CAT 3560 48': 'Catalyst 3560-48',
        # 'CAT 3560G 24': 'Catalyst 3560G-24',
        # 'CAT 3560G 48': 'Catalyst 3560G-48',
        # 'CAT 3750 24': 'Catalyst 3750-24TS-1U',
        # 'CAT 3750 48': 'Catalyst 3750-48TS',
        # 'CAT 3750E 24PS': 'Catalyst 3750E-24PS',
        # 'CAT 3750G 12S': 'Catalyst 3750G-12S',
        # 'CAT 3750G 24': 'Catalyst 3750G-24',
        # 'CAT 3750V2 24P': 'Catalyst 3750V2-24P',
        # 'CAT 3750X 24P': 'Catalyst 3750X-24P',
        # 'CAT 3750X 48 P': 'Catalyst 3750X-48P',
        # 'CAT 3750X 48 T': 'Catalyst 3750X-48T',
        # 'CAT 4006': 'Catalyst 4006',
        # 'CAT 6000 MSFC3': 'Catalyst 6000',
        # 'CAT 6506': 'Catalyst 6506',
        # 'CAT 6509': 'Catalyst 6509',
        '7200': '7206VXR',
        'N1912a': 'N1912A',

        'StorEdge T4': 'StorEdge 6120',

        'FT72 B7015': 'FT72-B7015',
        'X8DTG D': 'X8DTG-D',
        'SYS-5015A-PHF': 'X9SCD',
        'SYS-6016XT-TF': 'X8DTG-D',
        'SYS-6016T-6RTF+': 'X8DTU 6+',
        '6025W-URB-2U': 'X7DWU',
        'VIRTUAL MACHINE': 'Virtual Machine',
        
        '732-5': 'SYS-5038A-IL',
        
        'BA30A-AA': 'Ba30a-aa',
        'BA30P-AA': 'Ba30a-aa',
        
        'NETRA T1': 'Netra T1',
        '7148SX': 'DCS-7148SX',
    }, 
    'manufacturer': {
        'SUN MICROSYSTEMS': 'Sun Microsystems',
        'SUN / ORACLE': 'Sun Microsystems',
        'SUN': 'Sun Microsystems',
        'SUN MICROSYSTEMS, INC': 'Sun Microsystems',
        'CISCO': 'Cisco Systems',
        'CISCO SYSTEMS': 'Cisco Systems',
        # 'AMERICAN MEGATRENDS INC.': 'Sun Microsystems',
        'PHOENIX TECHNOLOGIES LTD.': 'Sun Microsystems',
        'DELL COMPUTER CORPORATION': 'Dell',
        'DELL COMPUTER CORP': 'Dell',
        'DELL INC': 'Dell',
        'DELL INC.': 'Dell',
        'DELLINC.': 'Dell',
        'DELL__': 'Dell',
        'DELL': 'Dell',
        'F5': 'F5 Networks',
        'HEWLETT PACKARD CO': 'HP',
        'HEWLETT-PACKARD': 'HP',
        'HP/COMPAQ': 'HP',
        'HP-PAVILION': 'HP',
        'APPLE': 'Apple',
        'APPLE INC.': 'Apple',
        'APPLE COMPUTER CO': 'Apple',
        'HONEYWELL': 'Honeywell',
        'DIGI INTERNATIONAL': 'Digi',
        'LENOVO': 'Lenovo',
        'International Business Mach': 'IBM',
        'AMAX': 'Supermicro',
        'SUPERMICRO': 'Supermicro',
        'SUPER MICRO COMPUTER': 'Supermicro',
        'BROCADE COMMUNICATIONS': 'Brocade',
        'BROCADE': 'Brocade',
        'CANON': 'Canon',
        'MOTOROLA': 'Motorola',
        'ACER': 'Acer',
        'ARISTA': 'Arista',
        'AVOCENT': 'Avocent',
        'MICROSOFT': 'Microsoft Corporation',
        'DIGI': 'Digi',
        'COLDFIRE': 'Coldfire',
        'SLAC': 'SLAC',
        'XEROX': 'Xerox Corporation',
        'XEROX CORP': 'Xerox Corporation',
        'ALLEN BRADLEY': 'Allen Bradley',
        'AXIS': 'Axis',
        'AGILENT TECHNOLOGIES': 'Agilent Technologies',
        'AGILENT': 'Agilent Technologies',
        'AMERICAN POWER CONVERSION,APC': 'APC',
        'NEWPORT': 'Newport',
        'SONY CORP': 'Sony Corporation',
        'COMPAQ COMPUTER CORP': 'Compaq Computer Corporation',
        'DIGITAL EQMT CO': 'Digital Equipment Corporation',
        'DEC': 'Digital Equipment Corporation',
        'LAWRENCE LIVERMORE RAD LAB': 'LBNL',
        'KONICA': 'Konica Minolta',
        'VMWARE': 'VMware, Inc.',
        'RACKABLE': 'Rackable Systems',
        'NETAPP': 'NetApp',
        'TYAN': 'Tyan',
        'EPSON': 'Epson',
        'LACIE': 'Lacie',
        'NATIONAL INSTRUMENTS': 'National Instruments',
        'NAT': 'National Instruments',
        'TEKTRONIX': 'Tektronix, Inc',
        'VA LINUX': 'VA Linux',
        'VADATECH': 'VadaTech',
        'KIP AMERICA': 'KIP',
        'PALO ALTO NETWORKS': 'Palo Alto Networks',
    },
    'device_type': {
        
        'UNIX-SRVR': 'unix server',
        'LINUX SRVR': 'linux server',

        'WIN-SRVR': 'windows server',
        
        'FILE-SRVR': 'storage server',
        'RAID': 'storage',
        'SAN': 'storage',
        'TAPE-LIB': 'storage',
        'STORAGE SYSTEM': 'storage',
        
        'SERVER BLADE': 'server',
        'VAX': 'server',
        'MAC-SRVR': 'server',
        'VMS-SRVR': 'server',
        'SERVER': 'server',
        
        'VM-SERVER': 'server',
        'VIRT-NET': 'server',

        'SWITCH-HUB': 'switch',
        'SWITCH': 'switch',
        'ROUTER': 'router',
        'FIREWALL': 'router',
        'SAN-SWITCH': 'switch',

        'AV': 'camera',
        'CAMERA': 'camera',
        'WEBCAM': 'camera',
        
        'PRINTER': 'printer',
        'SCANNER': 'printer',

        'LAPTOP': 'laptop',

        'PC': 'computer',
        'LINUX-WKS': 'computer',
        'MICROCOMPUTER': 'computer',
        'MAC': 'computer',
        'ALC': 'computer',

        'ATTOCUBE': 'computer',
        'AXP-SERVE': 'computer',
        'AXP-WKS': 'computer',
        'BECKHOFF': 'computer',
        'DEC-WKS': 'computer',
        'ETHERMETER': 'computer',
        'HP-WKS': 'computer',

        'HVAC': 'computer',
        'IDS/IPS': 'router',
        'LAN-MISC': 'computer',

        'O-SCOPE': 'computer', # instrument

        'PC-MISC': 'computer',
        'POE INJHUB': 'computer',
        'SIGNAL GEN': 'computer',
        'SITE-CTRL': 'computer',
        'SPEC ANLYZ': 'computer',
        
        'VAX-WKS': 'computer',
        
        'COMM-SRVR': 'computer',
        
        'XTERM': 'computer',
        'SUNRAY': 'computer',
        'SUN-WKS': 'computer',

        'TERM-SRVR': 'computer',
        'TEST-EQUIP': 'computer',
        'TOUCH PNL': 'computer',
        'USB-SERVER': 'computer',
        'VAS-WKS': 'computer',

        'PLC': 'computer',
        'PDU': 'power',
        'PWR-EQUIP': 'power',
        'UPS': 'power',
        
        'IOC': 'controller',
        'VME': 'controller',
        'VME CRATE': 'controller',
        'CPU-CARD': 'controller',
        'DAQ-INST': 'controller',
        'MCH': 'controller',
        'MKSU': 'controller',
        'BPM RCVR': 'controller',
        'CAMAC': 'controller',
        'CONTROLLER': 'controller',
        'TEMP-CTRL': 'controller',

        'EPSC': 'computer',
        
        'ATTOCUBE': 'computer',
        'W-MISC': 'computer',

    },
    'owner': {
        'SSRL SMB RESEARCH AND USER': 'SSRL',
        'SLAC/SSRL': 'SSRL',
    },
    'os:name': {
        'WINDOWS': 'Microsoft Windows',
        'WINDOWS': 'Microsoft Windows', 
        'WINDOWS-2000': 'Microsoft Windows',
        'WINDOWS-2003': 'Microsoft Windows',
        'WINDOWS-2008': 'Microsoft Windows',
        'WINDOWS-2008-R2': 'Microsoft Windows',
        'WINDOWS-2012': 'Microsoft Windows',
        'WINDOWS-7': 'Microsoft Windows',
        'WINDOWS-NT': 'Microsoft Windows',
        'WINDOWS-VISTA': 'Microsoft Windows',
        'WINDOWS-XP': 'Microsoft Windows',
        'WINDOWS-XP-SP2': 'Microsoft Windows',
        'MICROSOFT WINDOWS 7 ENTERPRISE': 'Microsoft Windows',
        'MICROSOFT WINDOWS 7 ENTERPRISE': 'Microsoft Windows',
        'MICROSOFT WINDOWS 7 PROFESSIONAL': 'Microsoft Windows',
        'MICROSOFT WINDOWS 8 ENTERPRISE': 'Microsoft Windows',
        'MICROSOFT WINDOWS 8.1 ENTERPRISE': 'Microsoft Windows',
        'MICROSOFT WINDOWS 8.1 PRO': 'Microsoft Windows',
        'MICROSOFT WINDOWS SERVER 2008 R2 DATACENTER': 'Microsoft Windows',
        'MICROSOFT WINDOWS SERVER 2008 R2 ENTERPRISE': 'Microsoft Windows',
        'MICROSOFT WINDOWS SERVER 2008 R2 STANDARD': 'Microsoft Windows',
        'MICROSOFT WINDOWS SERVER 2012 DATACENTER': 'Microsoft Windows',
        'MICROSOFT WINDOWS SERVER 2012 R2 STANDARD': 'Microsoft Windows',
        'MICROSOFT WINDOWS SERVER 2012 STANDARD': 'Microsoft Windows',
        'MICROSOFT WINDOWS XP PROFESSIONAL': 'Microsoft Windows',
        'MICROSOFT(R) WINDOWS(R) SERVER 2003 STANDARD X64 EDITION': 'Microsoft Windows',
        'MICROSOFT(R) WINDOWS(R) SERVER 2003, STANDARD EDITION': 'Microsoft Windows',
        'MICROSOFT(R) WINDOWS(R) XP PROFESSIONAL X64 EDITION': 'Microsoft Windows',
        'MICROSOFT® WINDOWS SERVER® 2008 ENTERPRISE': 'Microsoft Windows',
        'MICROSOFT® WINDOWS VISTA™ BUSINESS': 'Microsoft Windows',
        
        'HP-JETDIRECT': 'builtin',
        'PROPRIETARY': 'builtin',
        'XEROX-PRINTER': 'builtin',
        'RHEL': 'Redhat Enterprise Linux',
        'LINUX': 'Redhat Enterprise Linux',
        'SUN': 'Solaris',
        'SOLARIS': 'Solaris',
        'CISCO-IOS': 'Cisco IOS',
        'GOLDFIRE': 'Goldfire',
        'MACOSX': 'Apple Mac OSX',
        'MACOS': 'Apple Mac OS',
        'VMWARE-ESXI': 'VMWare ESXI'
    }
}


# map to admin
admin_map = [
    {
        'admin:username': {
            'bartelt': 'Unix',
            'renata': 'Unix',
            'ksa': 'Unix',
            'yemi': 'Unix',
            'shirley': 'Unix',
            'amay': 'Unix',
            'lnakata': 'Unix',
            'jonl': 'Unix',
            'ian': 'Unix',
            
            'bchoate': 'Facilities',
            'rafael': 'Facilities',

            'ovrahim': 'Site-Security',

            'jdomingo': 'ITDS',
            'nixx': 'ITDS',
            'sryan': 'ITDS',

            'antony': 'Network',
            'gcx': 'Network',
            'toddmtz': 'Pulse',
            
            'wchan': 'Windows',
            'neal': 'Windows',
            'timm': 'Windows',
            'rodwin': 'Windows',
            'ssink': 'Windows',
            'jcpierre': 'Windows',
            'mattc': 'Windows',
            
            'hlwin': 'Email',
            'rkau': 'Email',
            'juliyana': 'ERP',
            
            'jingchen': 'MCC',
            'brobeck': 'MCC',
            'cxg': 'MCC',
            
            'perazzo': 'PCDS',
            'ling': 'PCDS',

            'ramirez': 'SSRL',
            'thomas.eriksson': 'SSRL',
            'wermelsk': 'SSRL',
            'winston': 'SSRL',

            'fransham': 'Babar',
            'luitz': 'Babar',

            'becla': 'LSST',

        } 
    },
    { 
        'subnet': {
            compile('FACET.*'): 'MCC',
            'ACCTESTFAC': 'MCC',
            compile('LCLS*'): 'MCC',
            'SLCLAVC': 'MCC',
            'LEB': 'MCC',
            compile('BBRMMS-*'): 'Babar',
            compile('FWSSRL1-.*'): 'SSRL', 
            'APMGMT-SSRL': 'SSRL',
            'SSRL': 'SSRL',
            'SSRLA': 'SSRL',
            compile('B084F1-LAT.*'): 'PCDS',
            'DEVCTL-PCDSN': 'PCDS',
            'DEVCTL-B050': 'Unix',
            compile('LTDA.*'): 'Babar',
            compile('FARM.*'): 'Unix',
            compile('PCDSN-.*'): 'PCDS',
            compile('REG-L.*'): 'PCDS',
            compile('B\w+-PRINTER'): 'ITDS',
            compile('B\w+-PUBLIC'): 'ITDS',
            compile('B\w+-PRIVATE'): 'ITDS',
            'PUB4': 'ITDS',
            'BSD': 'ITDS',
            'KAVLI': 'ITDS',
            'SSRL-PRINTER': 'ITDS',
            compile('.*-SITESEC-.*'): 'Site-Security',
            compile('.*-POWER'): 'Facilities',
            'FW-HVAC': 'Facilities',
            'FW-HVAC-ACC': 'Facilities',
            compile('EPN.*'): 'ERP',
            compile('^ERP.*'): 'ERP',
            'DEVCTL-PULSE': 'Unix',
            'ESATESTFAC': 'ITDS',
            compile('NETMGMT.*'): 'Network',
            'NETMGMT-SSRL': 'SSRL',
            compile('NLCTA.*'): 'NLCTA',
            compile('PBX.*'): 'Telecoms',
            compile('SERV01-SEC-.*'): 'Cyber-Security',
            'SERV01-CPP-SENSOR': 'Cyber-Security',
            compile('SERV01-SEC-PRIV\d+'): 'Cyber-Security',
        }
    },
    {
        'port:hostname': {
            compile('(SP-)?WAIN\d*'): 'Unix',
            compile('SP-.*'): 'Unix',
            compile('SEC-.*'): 'Cyber-Security',
            compile('NET-.*'): 'Network',
            compile('.*SSRL.*'): 'SSRL',
        }
    },
    { 
        'device_type': {
            # 'unix server': 'unix-admin',
            'power': 'Facilities',
            # 'windows server': 'windows-admin',
            'router': 'Network',
            'switch': 'Network',
            'load balancer': 'Network',
        } 
    }
]


def determine_admin_group( item, strategies=admin_map ):
    groups = []
    logging.debug("determine admin")
    for i,strategy in enumerate(strategies):
        for k,d in strategy.iteritems():
            # logging.error("K: %s, item %s" % (k,item))
            if k in item:
                a = item[k]
                if not isinstance(a,list):
                    a = [ a, ]
                for v in a:
                    # logging.debug(" using strategy %s: %s, value %s" % (i,k,v))
                    if v in strategy[k]:
                        # logging.debug( " matching on static key: %s" % (v,))
                        if not strategy[k][v] in groups:
                            logging.debug(' found %s by static map using %s' % (strategy[k][v],i))
                            groups.append( strategy[k][v] )
                        continue
                    else:
                        for r in strategy[k]:
                            if not isinstance(r,str) and v:
                                # logging.debug(' matching on regex key: %s' %(r.pattern))
                                if r.match( v ):
                                    logging.debug("  found %s by regex %s using %s" % (strategy[k][r],r.pattern,i))
                                    if not strategy[k][r] in groups:
                                        groups.append( strategy[k][r])
                                    continue
    # logging.error("GROUP: %s" % (groups,))
    if len(groups) == 0:
        # assume ITDS
        groups.append('ITDS')
    if 'Network' in groups:
        groups = [ 'Network', ]
    elif 'MCC' in groups:
        groups = [ 'MCC', ]
    return groups


###
# tools
###
def mac_address( m, format='host' ):
    m = m.replace('.','').replace(':','')
    if format == 'host':
        a = [ m[i:i+2] for i in xrange(0,len(m),2) ]
        return ':'.join(a)
    return m


def merge_dict( old, new, name, ignore=() ):
    ignored = {}
    for k,ref in new.iteritems():
        v = {}
        try:
            for i,j in ref.iteritems():
                v[i] = j
                v['db'] = name
        except:
            v = { 'value': ref, 'db': name }

        if k in ignore:
            if not k in ignored:
                ignored[k] = []
            ignored[k].append( v )

        else:
            if k and not k in old:
                old[k] = []
            old[k].append( v )
            
    return old, ignored


def get_search_list( field, d ):
    # find all unique values of d from field
    # logging.error("D %s: %s" % (field,d))
    v = []
    for i in d[field[0]]:
        f = 'value'
        try:
            f = field[1]
        except:
            pass
        if f in i:
            if i[f] not in v:
                if not i[f] == None:
                    v.append( i[f])
    if len(v) == 1:
        return v.pop()
    elif len(v) == 0:
        raise LookupError, 'not matching fields'

    return { '$in': v }

    # raise NotImplementedError('need IN search for %s = %s' % (field,v))



def merge_item( dbs, db, fields, d, recent_only=False, ignore=None ):
    matched_count = 0
    try:
        # found matches
        count = 0

        v = get_search_list( fields, d )
        search_field = '.'.join(fields)
        s = { search_field: v }
    
        # logging.debug(" + looking for %s" % (s,))
        found = [ o for o in dbs[db].find( s ) ]
        count = len(found)
        logging.debug(' + %s had %s using\t%s' % (db,count,s))
        # if recent_only, then pick the most recent if more than one found
        if count > 1 and recent_only:
            # logging.error("RECENT ONLY")
            t = None
            for n,f in enumerate(found):
                # logging.debug("  - %s %s" % (db,f,))
                if t == None:
                    t = f
                if f['updated_at'] > t['updated_at']:
                    t = f
            found = [t,]
            # found = found.pop(0)
            
        # branch for special cases where we only want subset of data
        branched = False
        # logging.error("FOUND: %s" % (found,))
        if len(found) and ignore and isinstance( ignore, dict ):
            # merge into a sub data structure
            # logging.error("  IGNORE: %s" % (ignore))
            for x,y in ignore.iteritems():
                field,sub = split_field(x)
                # logging.debug("  ignore x: %s (%s:%s), y: %s" % (x,field,sub,y))
                if field in d:
                    # logging.debug("   d contains %s => %s" % (x,d[field]))
                    for a,b in y.iteritems():
                        # logging.debug("     a=%s\tb=%s: %s: %s" % (a,b,field,d[field]))
                        for n in d[field]:
                            # logging.debug("   d[field]=%s, a=%s, b=%s" % (n,a,b))
                            if sub in n and n[sub] == a:
                                # logging.debug("  ignoring... %s" % (b,))
                                for f in found:
                                    d, ignored = merge_dict( d, f, db, ignore=b )
                                    d = remove_dups( d )
                                # logging.debug("   reduced to: %s" % (d,))
                                branched = True
            # stupid hack to get back the hostname
            for f in found:
                this_d = {
                    'port': { 'hostname': f['port']['hostname'] }
                }
                if 'ip_address' in f['port']:
                    this_d['port']['ip_address'] = f['port']['ip_address']
                logging.debug("  get back vm hostname from %s" % (this_d,))
                d, ignored = merge_dict( d, this_d, 'cando' )

        # otherwise just do a full merge
        if not branched and len(found):
            for f in found:
                # logging.debug("  - %s %s" % (db,f,))
                d, ignored = merge_dict( d, f, db )
                matched_count = matched_count + 1

    except LookupError, e:
        pass
    except NotImplementedError, e:
        logging.error("ERR: %s" % (e,))

    # logging.error("OUT: %s" % (d,))
    return matched_count, d

def stringify(dict, exclude=[]):
    return ';'.join( [ "%s=%s" % (f,v) for f,v in dict.iteritems() if not f in exclude ] )

def unstringify( s ):
    d = {}
    for k,v in s.split(';').iteritems():
        d[k] = v
    return d

def remove_dups( item ):
    """ goes through each field and determines remove anon arrays content duplicates """
    out = {}
    # logging.error("IN: %s" % item )
    for k,array in item.iteritems():
        out[k] = []
        this_seen = {}
        # use a string to hold hash
        for d in array:
            s = ';'.join( [ "%s=%s" % (f,v) for f,v in d.iteritems() ] )
            if not s in this_seen:
                out[k].append( d )
            this_seen[s] = True
    # logging.error("OUT: %s\n" % out )
    return out

def merge( mongo, ips=[], subnets={}, null_char='', db_names=[], ensure_indexes=( 'nodename.value', 'port.mac_address', 'port.ip_address', 'serial.value', 'PC.value' ), strategies = {
        'by_mac_address': {
            'start_db': 'ptolemy_arp',
            'tactics': [ 
                # try to find the mac address for the found ip address using ptolemy
                # then fill in the rest based from the mac address
                ( 'sccm', [ 'port', 'mac_address' ], True ),
                ( 'taylor', [ 'port', 'mac_address' ] ),
                ( 'dhcp', [ 'port', 'mac_address' ] ),
                # ( 'sccm', [ 'port', 'ip_address' ], True ),
                # ( 'taylor', [ 'port', 'ip_address' ] ),
                # ( 'cando', [ 'port', 'ip_address' ], False, { 'is_vm:value': { True: ( 'nodename', 'os', 'manufacturer', 'model', 'PC', 'PO', 'port' ) } } ), # no recent, ignore vm's
                ( 'cando', [ 'port', 'ip_address' ], False, { 'is_vm:value': { True: ( 'nodename', 'os', 'manufacturer', 'model', 'PC', 'PO' ) } } ), # no recent, ignore vm's
                ( 'ptolemy_device', [ 'nodename' ] ),
                ( 'bis', [ 'serial' ] ),
                ( 'bis', [ 'PC' ] ),
                ( 'dhcp', [ 'PC', ] ),
                ( 'rackwise', [ 'serial', ] ),
                ( 'rackwise', [ 'PC', ] ),
            ],
        },
    } ):
    """ 
    use referential transparency to simplify the merging
    basically keep the same datastructure for all sources of data in the form dict = { field1: [], field2: []...}
    each pass of a tactic will append onto the relevant field a dict containing all relevant values for that field
    it must also append a 'db' key as part of this hash to identify it's source
    we then store it back in to the document database for later collation
    """
    
    dbs = {}
    for i in db_names:
        logging.debug("initiating db %s" % (i,))
        dbs[i] = get_mongo_collection( mongo, i )
    
    # determine ips to filter for if requested
    search = [ { 'port.ip_address': i } for i in ips ]
    
    logging.info('merging items with %s' % (search,))
    good = 0
    bad = 0
    total = 0
    print "nodename\tadmin\tuser\thostname\tdhcp\tdhcp_ip_address\tip_address\tmac_address\tsubnet\tupdated_at\tage"
    
    now = datetime.now()
    
    for name, strategy in strategies.iteritems():

        logging.info("strategy: %s %s" % (name, strategy))
        # clear staging db
        dbs[name] = get_mongo_collection( mongo, name )
        dbs[name].remove()

        for i in ensure_indexes:
            dbs[name].ensure_index( i, 1  )

        it = dbs[strategy['start_db']].find()
        if len(search):
            it = dbs[strategy['start_db']].find( { '$or': search } )

        for i in it:

            total = total + 1

            d, ignored = merge_dict( {}, i, strategy['start_db'] )
            logging.debug( 'found to merge %s' % (d,) )

            okay = False
            for n, tactic in enumerate(strategy['tactics']):
                # logging.debug(" trying tactic %s:\t%s" % (n,tactic))
                r = False
                v = False
                try:
                    r = tactic['time']
                    v = tactic['filter']
                except:
                    pass
                c, d = merge_item( dbs, tactic['db'], tactic['fields'], d, recent_only=r, ignore=v )
                if c > 0:
                    d = remove_dups( d )
                    okay = True

            # make sure we clear the id - wil cause insert issues otherwise
            del d['_id']
            # logging.error("d (%s): %s" % (okay,d,))

            if okay:

                good = good + 1

                # lookout for funny duplicate entries for wifi card and etherenet on like laptops
                # this comes out as having two mac addresses, one of which only have dhcp but not nodename as
                # this is not stored as part of the dhcp
                mac_addresses = {}
                if 'port' in d:
                    for p in d['port']:
                        if 'mac_address' in p:
                            mac_addresses[p['mac_address']] = True
                if len(mac_addresses) > 1:
                    # logging.error("POSSIBLE MAC ADDRESSES %s" % (mac_addresses,))
                    # do a search of the final database to see if we already have it somewhere
                    search = [ { 'port.mac_address': m } for m in mac_addresses.keys() ]
                    # f = False
                    for j in dbs[name].find( { '$or': search } ):
                        # logging.error("==> J: %s" % (j,))
                        for x,y in j.iteritems():
                            if x == '_id':
                                continue
                            if not x in d:
                                d[x] = y
                            else:
                                for z in y:
                                    d[x].append( z )
                        # f = True
                        dbs[name].remove( j )
                        
                    d = remove_dups( d )

                if len(search):
                    logging.debug("merged: %s" % (d,))
                dbs[name].insert( d )

            else:

                bad = bad + 1
                subnet = null_char
                try:
                    subnet = ip_in_subnet(d['port'][0]['ip_address'],subnets)['name']
                except:
                    pass
                # logging.error("D: %s" % (d,))
                try:
                    days_ago = null_char
                    ceil( (now - d['updated_at'][0]['value']).days / 7 )
                except:
                    pass
                    
                print "%s\t%s, %s\t%s, %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (
                    d['nodename'][0]['value'] if 'nodename' in d else null_char,
                    d['admin'][0]['lastname'] if 'admin' in d else null_char, d['admin'][0]['firstname'] if 'admin' in d else null_char,
                    d['user'][0]['lastname'] if 'user' in d else null_char, d['user'][0]['firstname'] if 'user' in d else null_char,
                    d['port'][0]['hostname'] if 'port' in d and 'hostname' in d['port'][0] else null_char,
                    d['port'][0]['dhcp'] if 'port' in d and 'dhcp' in d['port'][0] else null_char,
                    d['port'][0]['dhcp_ip_address'] if 'port' in d and 'dhcp_ip_address' in d['port'][0] else null_char,
                    d['port'][0]['ip_address'] if 'port' in d and 'ip_address' in d['port'][0] else null_char,
                    d['port'][0]['mac_address'] if 'port' in d and 'mac_address' in d['port'][0] else null_char,
                    subnet,
                    d['updated_at'][0]['value'] if 'updated_at' in d else null_char,
                    days_ago
                )
            logging.debug("\n")

    logging.info("PERCENT BAD: %s (%s/%s)" % (bad*100/total, bad, total) )


def merge_other( mongo, frm='cando', to='by_mac_address', subnets={}, init_search={ 'port.ip_address': ['port','ip_address'] }, merge_into_search={ 'nodename.value': ['nodename',] }, seen_key=[ ['port', 'ip_address'], ['nodename',], [ 'port', 'hostname' ] ] ):
    # do a final scan through cando to get entries not matched
    dbs = {}
    dbs[frm] = get_mongo_collection( mongo, frm )
    dbs[to] = get_mongo_collection( mongo, to )

    # dbs['staging'] = get_mongo_collection( mongo, 'staging' )
    # dbs['staging'].remove()
    
    need_to_merge = 0
    need_to_add = 0
    total = 0
    skipped = 0
    seen = {}
    
    def string_tuple( r ):
        out = []
        for s in seen_key:
            this = r
            for i in s:
                # logging.error("%s\t%s" % (i,this))
                this = this.get(i)
            out.append(this)
        # return '%s\t%s\t%s' % (r['port']['ip_address'], r['nodename'], r['port']['hostname'])
        return ':'.join(out)
        
    def construct_search( r, d ):
        search = {}
        for k,v in d.iteritems():
            this = r
            for i in v:
                this = this.get(i)
            search[k] = this
        return search
    
    for r in dbs[frm].find():
        
        total = total + 1 

        # logging.info("=> %s\t%s" % (ip,hostname))

        # look up the ip to see if we have a matching one in 'to' db
        search = construct_search( r, init_search )
        # search[k] = 'port.ip_address': r['port']['ip_address'] }

        found = False
        for d in dbs[to].find( search ):
            found = True

        if found == False:
            
            # remove dups
            s = string_tuple(r)
            if s in seen:
                skipped = skipped + 1
                continue

            seen[s] = True

            action = None
            merge = []

            # check up against the nodename, if exists then add as interface
            # search = { 'nodename.value': r['nodename'] }
            search = construct_search( r, merge_into_search )
            # logging.error("SEARCH: %s" % (search,))
            merge = [ d for d in dbs[to].find( search ) ]
            if len(merge):
                action = 'merge'

            # create collapsed data structure for storage
            m, ignored = merge_dict( {}, r, frm )
            del m['_id']

            if action == 'merge':
                
                # for merge ndoes, we don't know for sure that the nodenames are good
                # as we actually need to perform the collate/report first to get a good idea of the 
                # real nodenames.
                need_to_merge = need_to_merge + 1
                logging.debug("MERGE    %s" % (m,))
                dbs[to].insert( m )

            else:

                # for new nodes, we have a high confidence that's it's not already there, so just
                # add it to the to db.
                need_to_add = need_to_add + 1
                logging.debug("NEW NODE %s" % (m,))
                dbs[to].insert( m )

    logging.info("%s: total=%s, add=%s (%s), merge=%s (%s), skipped=%s" % (frm, total, need_to_add, need_to_add*100/total, need_to_merge, need_to_merge*100/total, skipped) )


def get_subnets( cursor ):
    cursor.execute( """SELECT *
    FROM
      subnets
    """ )
    subnets = {}
    for d in cursor:
        i = d['prefix'] + '/' + d['netmask']
        # print ' %s' % (i,)
        try:
            k = ipaddress.ip_network(i)
            s = search( r'vlan\s*(?P<vlan>\d+)', d['description'], IGNORECASE )
            d['vlan'] = None
            if s:
                d['vlan'] = s.groupdict()['vlan']
            # logging.error("VLAN: %s" % (d,))
            subnets[k] = d
        except:
            pass
    return subnets


def ip_in_subnet( ip, subnets ):
    # logging.debug("ip subnet lookup: %s\t%s" % (ip,subnets))
    for k,d in subnets.iteritems():
        if ipaddress.ip_address(ip) in k:
            return d
    return None


def collapse( l ):
    this = {}
    for d in l:
        # logging.error("  D %s" %d)
        for k,v in d.iteritems():
            if not k in this:
                this[k] = {}
            
            if not isinstance( v, list ):
                v = [ v ]
            for w in v:
                # logging.warn("THIS[%s] = %s,\t w=%s,\t l=%s" % (k,this[k],w,l))
                if not w in this[k]:
                    this[k][w] = []
                if not d['db'] in this[k][w]:
                    this[k][w].append( d['db'] )
    del this['db']
    
    return this
    
    
def collate_item( item, null_char='',
        fields=( 'device_type', 'is_vm', 'nodename', 'subnet', 'user', 'admin', 'custodian', 'owner', 'disk', 'memory', 'cpu', 'manufacturer', 'model', 'serial', 'PC', 'PO', 'location', 'warranty', 'os', 'updated_at' ),
        remap=CONTENT_REMAP
    ):
    # 'port', 
    
    """ from the merge() output, flatten the values for each field and determine if there are any conflicts """
    summary = {}
    # logging.error("FIELD: %s" % (item,))
    for s in fields:

        summary[s] = { 'data': None, 'status': None }

        try:
            # logging.error("S: %s" % (s,))
            summary[s]['data'] = collapse( item[s] )
        except KeyError, e:
            # logging.debug("ERROR: %s" % (e,))
            pass

        # add PC number if name kinda looks right
        if s == 'PC':
            try:
                logging.debug(" post parsing pc number for %s" % (summary['nodename']['data']['value'],))
                for k in summary['nodename']['data']['value']:
                    if search( r'-PC\d+$', k ):
                        n = parse_number( k, prepend='PC', digits=5 )
                        if n:
                            # logging.error(" THIS: %s", summary[s]['data'] )
                            if not summary[s]['data']:
                                summary[s]['data'] = { 'value': {} }
                            if not n in summary[s]['data']['value']:
                                summary[s]['data']['value'][n] = []
                            summary[s]['data']['value'][n].append( 'post' )
            except:
                pass
                
        elif s == 'updated_at': # and len(summary[s]['data']['value'].keys()):
            # treat updated_at - use most recent value
            try:
                values = [ n for n in summary[s]['data']['value'].keys() if n ]
            except:
                values = []
            if len(values):
                dt = max( values )
                # logging.error("DATETIME: %s (%s)" % (dt,summary[s]['data']['value'].keys()))
                summary[s]['data']['value'] = {}
                summary[s]['data']['value'][dt] = [ 'post', ]


        if summary[s]['data']:

            # logging.debug(" collating %s:\t %s" % (s,summary[s]['data']))
            status = True

            # two phase due to remapping
            # remap the values to consistent normalised texts
            for k in summary[s]['data']:
                
                v = summary[s]['data'][k]
                for i in v.keys():

                    j = v[i]
                    if i == None:
                        del summary[s]['data'][k][None]
                    
                    try:
                        z = i.upper()
                    except:
                        z = i
                    # logging.debug("    i: %s (%s)\tj: %s" % (z,i,j))
                    # if s in remap:
                    #     logging.debug("      remap: %s" % (remap[s],))
                    # remap common similar values
                    t = s
                    if not k == 'value':
                        t = '%s:%s' % (s,k)
                        # logging.error("T: %s %s - %s %s" % (k,t, t in remap,z))
                    
                    if t in remap and z in remap[t]:
                        # logging.debug("      remap!! %s" % (remap[s],))
                        # see if we have dest value already
                        new = remap[t][z]
                        if new in summary[s]['data'][k]:
                            logging.debug("    appending %s -> %s \t%s" % (i,new,summary[s]['data'][k]))
                            # logging.error("A: %s\t%s" % ('',summary[s]['data'][k][i]))
                            for a in summary[s]['data'][k][i]:
                                if not a in summary[s]['data'][k][new]:
                                    # logging.error(" HERE")
                                    summary[s]['data'][k][new].append( a )
                        else:
                            logging.debug("    setting %s -> %s" % (i,new))
                            summary[s]['data'][k][new] = summary[s]['data'][k][i]
            
                        if not i == new:
                            del summary[s]['data'][k][i]

                # logging.debug("   out -> %s" % (summary[s]['data'],))
            
            for k,v in summary[s]['data'].iteritems():

                # logging.debug( "  k: %s\tv: %s" % (k,v))
                these_keys = v.keys()
                len_these_keys = len(these_keys)

                if s == 'port':

                    # if port has multiple ip's, and sccm is in all of them,  ignore the singel sccm value
                    if k == 'ip_address':
                        # logging.error("UPDATED: %s %s" % (summary,summary[s]['data'],))
                        w = summary[s]['data'][k].keys()
                        if len( w ) == 2:
                            logging.debug("multiple sccm ip addresses")
                            sccm = 0
                            j = None
                            for i in w:
                                # logging.error("W: %s" % i)
                                if 'sccm' in summary[s]['data'][k][i]:
                                    sccm = sccm + 1
                                    if len(summary[s]['data'][k][i]) == 1:
                                        j = i
                            if sccm == 2 and j:
                                logging.debug("removing sccm ip address %s" % (j,))
                                del summary[s]['data'][k][j]
                        
                    # if we have both dhcp true and false, assume tru
                    if 'dhcp' in summary[s]['data']:
                        # logging.error("HERE: %s" % (summary[s]['data']['dhcp'],))
                        if True in summary[s]['data']['dhcp'] and False in summary[s]['data']['dhcp']:
                            del summary[s]['data']['dhcp'][False]
                
                # owner's SLAC is rather useless
                elif s == 'owner' and len_these_keys > 1:
                    logging.debug("  many owners")
                    # delete SLAC
                    try:
                        del summary[s]['data'][k]['SLAC']
                    except:
                        pass
                        
                # cando models and users shouldn't be trusted if we have other sources
                elif s in ( 'model', 'user', 'manufacturer', 'PC', 'os', 'location' ) and len_these_keys > 1:
                    logging.debug("  many %s's found" % (s,))
                    bis_room = []
                    for i in v.keys():
                        # trust bis location information
                        if s == 'location' and k == 'room': # and 'bis' in v[i]:
                            #logging.error("BIS ROOM i %s k %s d %s" % (i,k,summary[s]['data'][k],))
                            if 'bis' in v[i]:
                                bis_room.append(i)
                        if 'cando' in v[i] and len(v[i]) == 1:
                            logging.debug("   ignoring cando %s value %s from %s" % (s,i,summary[s]['data'][k]))
                            del summary[s]['data'][k][i]
                    if len(bis_room):
                        to_del = list( set(v.keys()) - set(bis_room) )
                        #logging.error("BIS %s (%s) -> %s: %s" % (bis_room,v.keys(),to_del,summary[s]['data']['room']))
                        for i in to_del:
                            del summary[s]['data']['room'][i]
                            
                elif s == 'device_type':
                    for i in v.keys():
                        if len_these_keys > 1 and 'rackwise' in v[i] and len(v[i]) == 1:
                            del summary[s]['data'][k][i]
                            
                # logging.warn("   fi: %s\t%s\t%s" % (s,k,v))
                if len( summary[s]['data'][k].keys() ) > 1:
                    status = False
                    break

            summary[s]['status'] = status
            logging.debug(" + collated: %s\t%s" % (s,summary[s],))

    # add timestamps to ports
    # logging.info("TIME: %s" % (summary,))

    
    # deal with funny hosts with multiple ip's as each hostname may be expressed out as a nodename by the various databases
    try:
        if summary['nodename']['status'] == False:
            nodenames = summary['nodename']['data']['value'].keys()
            hostnames = summary['port']['data']['hostname'].keys()
            # hardcode stupid bullets to be excluded
            if len(nodenames) == 2 and len(hostnames) == 1 and not ( hostnames[0].startswith('BULLET') or hostnames[0].startswith('SIMES') ):
                # hmmm
                ok = True
                for n in nodenames:
                    if null_char in n or ';' in n:
                        ok = False
                if ok and hostnames[0] in nodenames:
                    logging.debug( 'dealing with multiple nodenames NODES: %s\t HOSTS: %s' % (nodenames,hostnames) )
                    good = list( set( nodenames ) - set( hostnames ) ).pop(0)
                    # it's fine, just remove and add database to good value tally
                    for i in summary['nodename']['data']['value'][hostnames[0]]:
                        summary['nodename']['data']['value'][good].append( i )
                    del summary['nodename']['data']['value'][hostnames[0]]
                    summary['nodename']['status'] = True
    except:
        pass


    # dhcp databae custodians and admins are crap, delete if other sources available
    try:
        for f in ( 'admin', 'user', 'custodian' ):
            for sub in summary[f]['data'].keys():
                # logging.debug("F: %s, SUB: %s" % (f,sub))
                keys = summary[f]['data'][sub].keys()
                # logging.debug("  keys: %s, %s" % (keys,summary[f]['data'][sub]))
                if len(keys) > 1:
                    for k in keys:
                        if 'dhcp' in summary[f]['data'][sub][k]:
                            # logging.debug("deleting bad dhcp entries for f=%s sub=%s k=%s, %s" % (f,sub,k,summary[f]['data'][sub][k]))
                            del summary[f]['data'][sub][k]
    except:
        pass


    # logging.error("NODE: %s" % ( summary,))

    nodenames = []
    try:
        nodenames = summary['nodename']['data']['value'].keys()
        # logging.error("NODENAMES: %s"  % (nodenames,) )
    except:
        pass
        
    # logging.error("SUMMARY: %s" % (summary,))
    
    has_port_data = False
    if 'port' in summary and 'data' in summary['port'] and summary['port']['data']:
        has_port_data = True
    
    if len(set( ['ERP-FDEV-WEB01', 'ERP-FDEV-WEBX01', 'ERP-FDEV-WEB02', 'ERP-FUAT-WEB01', 'ERP-FUAT-WEB02', 'ERP-FPRD-WEBX01', 'ERP-FPRD-WEBX01', 'ERP-HDEV-WEB01', 'ERP-HDEV-WEB02', 'ERP-HPRD-WEB01', 'ERP-HPRD-WEB02', 'ERP-HUAT-WEBX01', 'PSHR-WEB01', 'PSHR-WEB02', 'PSOFT-ORACLE09', 'PSOFT-ORACLE10', 'PSOFT-ORACLE08', 'PSOFT-ORACLE11', 'SLAC-ORACLE01', 'COBRA-18', 'MYSQL01', 'MYSQL03', 'PSOFT-WEB01', 'PSOFT-WEB02' ] ).intersection( nodenames ) ) > 0:
    
        # logging.error("THIS: %s" % (item,))
        try:
    
            all_ip = summary['port']['data']['ip_address'].keys()
            for m in summary['port']['data']['ip_address'].keys():
                this = deepcopy(summary)
                for o in set( all_ip ) - set( [m,] ):
                    if o in this['port']['data']['ip_address']:
                        # logging.error( " delete IP: %s" % (o,))
                        del this['port']['data']['ip_address'][o]
                    hostname_for_ip = []
                    for p in item['port']:
                        # logging.error("  port: %s" % (p,))
                        if 'ip_address' in p and p['ip_address'] == o:
                            if 'hostname' in p:
                                hostname_for_ip.append( p['hostname'] )
                    logging.debug("  delete this hostname: %s" % (hostname_for_ip,))
                    # logging.error("    FROM: %s" % (this,) )
                    for h in hostname_for_ip:
                        if h in this['port']['data']['hostname']:
                            del this['port']['data']['hostname'][h]
                this['port']['data']['mac_address'] = {}
                name = this['port']['data']['hostname'].keys().pop()
                this['nodename']['data']['value'] = { name: [ 'post', ]}
                # logging.error("IP: %s\t%s" % (m,name))
                logging.debug("collated multiple erp addresses: %s" % (name,) )
            
                yield this, True

        except:
            pass
    
    
    elif ( has_port_data and 'mac_address' in summary['port']['data'] and  summary['port']['data']['mac_address'] and len(summary['port']['data']['mac_address']) == 1 and 'ip_address' in summary['port']['data'] and summary['port']['data']['ip_address'] and len(summary['port']['data']['ip_address']) > 1 ):
            
            # assume each ip is separate interface on device
            all_ip = summary['port']['data']['ip_address'].keys()
            for i in all_ip:
                this = deepcopy(summary)
                if 'interface' in summary['port']['data']:
                    del summary['port']['data']['interface']
                for o in set( all_ip ) - set( [i,] ):
                    del this['port']['data']['ip_address'][o]
                    # need to determine appropriate hostname to delete too
                    # logging.error("delete %s from: %s" % (o,this['port'],))
                    hostname_for_ip = []
                    for p in item['port']:
                        # logging.error("  port: %s" % (p,))
                        if 'ip_address' in p and p['ip_address'] == o:
                            if 'hostname' in p:
                                hostname_for_ip.append( p['hostname'] )
                    logging.debug("  delete this hostname: %s" % (hostname_for_ip,))
                    # logging.error("    FROM: %s" % (this,) )
                    for h in hostname_for_ip:
                        if h in this['port']['data']['hostname']:
                            del this['port']['data']['hostname'][h]
                    # delete mac address if not same nodename not same as hostname
                if 'hostname' in this['port']['data'] and len(set( this['nodename']['data']['value'].keys() ).intersection( this['port']['data']['hostname'].keys() ) ) == 0:
                    this['port']['data']['mac_address'] = {}
                    
                logging.debug("collated multiple ip addresses %s" % (this,) )
                yield this, True

    
    # deal with entries with two mac addresses due to dhcp merge; assume they are valid
    elif has_port_data and 'mac_address' in summary['port']['data'] and len(summary['port']['data']['mac_address']) > 1:

        all_mac = summary['port']['data']['mac_address'].keys()
        for m in all_mac:
            this = deepcopy(summary)
            for o in set( all_mac ) - set( [m,] ):
                del this['port']['data']['mac_address'][o]
            logging.debug("collated multiple mac addresses" ) #% (this,) )
            yield this, False



    elif has_port_data and 'nodename' in summary and 'data' in summary['nodename'] and summary['nodename']['data'] and summary['nodename']['data']['value'] and len(set( ['WEB07','WEB01'] ).intersection( nodenames )):
        
        this = summary['port']['data']['hostname'].keys().pop()
        if this.startswith( 'WEB07-' ) or this.startswith( 'WEB01-' ):
            # logging.error("GOTCHA %s" % this)
            del summary['nodename']['data']['value']
            summary['nodename']['data']['value'] = { this: [ 'post', ] }
                        
        yield summary, False


    else:

        logging.debug("collated single" ) #": %s" % (summary,))
        yield summary, False




def split_field( f ):
    g = f
    sub = 'value'
    if ':' in f:
        g,_,sub = f.partition(':')
    return g, sub

def datasources( summary ):
    found = {}
    # logging.debug("SOURCES: %s" % (summary,))
    for f in summary.keys():
        # logging.error("THIS: %s" % (summary[f],))
        if isinstance( summary[f], list ):
            for x in summary[f]:
                if 'db' in x:
                    found[x['db']] = True
    return sorted( [ '%s'%f.encode('utf-8') for f in found.keys() ] )


def collate( cursor, sort_by, fields=[], subnets={}, null_char='' ):

    for i in cursor:
        
        logging.debug("")
        logging.debug( "collating %s" % (i,) )
        for summary, ignore_done in collate_item( i ):
            # logging.debug("summarising %s" % summary )

            # merge subnet
            try:
                # logging.error("SUBNET: %s" % (subnets,))
                ip = ','.join( summary['port']['data']['ip_address'].keys() )
                summary['subnet']['data'] = { 'value': { ip_in_subnet(ip,subnets)['name']: ['post',] } }
                summary['subnet']['status'] = True
            except:
                summary['subnet']['status'] = False

            try:
                # try to determine type of device
                category = None
                sources = {}
            
                # assume anything in rackwise is a server
                for x,y in summary['nodename']['data'].iteritems():
                    # logging.warn("Y: %s" % (y,))
                    for z, a in y.iteritems():
                        for s in a:
                            sources[s] = True
                        # logging.warn("Z: %s \t%s" % (z,a))
                        if z.startswith('RTR-LB'):
                            category = 'load balancer'
                        elif z.startswith( 'SWH-'):
                            category = 'switch'
                        elif z.startswith( 'RTR-') or z.startswith( 'RTRG-'):
                            category = 'router'
                        elif z.startswith( 'AP-' ):
                            category = 'access point'
                        elif 'rackwise' in a:
                            category = 'server'
                        if category:
                            break
                        
                # narrow down if server
                if category == 'server':
                    if 'taylor' in sources:
                        category = 'unix server'
                    elif 'goliath' in sources:
                        category = 'windows server'
                        
                # if subnet indicates type
                if category == None:
                    for x,y in summary['subnet']['data'].iteritems():
                        for k in y:
                            # logging.error("Y %s" % (k,))
                            if k.startswith( 'FARM' ):
                                category = 'unix server'
                            elif k.startswith( 'SERV' ) or k.startswith( 'NETHUB'):
                                category = 'server'
                            elif k.startswith( 'DEVCTL-' ) or k in ( 'LTDA-VM' ):
                                category = 'unix server'
                            elif 'WINMGMT' in k:
                                category = 'windows server'
                            elif 'PRINTER' in k:
                                category = 'printer'
                            if category:
                                break

                if category:                
                    if category in summary['device_type']['data']['value']:
                        summary['device_type']['data']['value'][type].append( 'post' )
                        for x in summary['device_type']['data']['value'].keys():
                            if not x == category:
                                del summary['device_type']['data']['value'][x]
                    else:
                        summary['device_type']['data']['value'] = { category: ['post',] }
                    summary['device_type']['status'] = True

            except:
                summary['device_type']['status'] = False
        
        
            # logging.debug( "summary: %s" % (summary,) )

            out = {}
        
            # first field is if the summary is ok
            state = []
            bad = {}
            
            # rest of data
            # logging.debug("== %s" % (summary,))
            for f in fields:
                g, sub = split_field( f )
                # logging.debug(" analysing %s.%s: %s" % (g,sub,summary[g]))
                value = None
                try:

                    this = summary[g]['data'][sub]
                    a = this.keys()
                    value = a #'; '.join( "%s"%i for i in a )
                    ok = True

                    if len(a) > 1:
                    
                        good = False
                    
                        # logging.error("LEN: %s %s " % (g,a,))
                        if g == 'nodename':

                            # deal with rackwise and blade chassis
                            rackwise_nodenames = []
                            other_names = []
                            # get name in rackwise
                            for x,z in this.iteritems():
                                if 'rackwise' in z:
                                    if not x in rackwise_nodenames:
                                        rackwise_nodenames.append(x)
                                else:
                                    if not x in other_names:
                                        other_names.append(x) 

                            # logging.error("RACK: %s" % (rackwise_nodenames,))
                            if len(rackwise_nodenames):
                            
                                # if we have common names, no problem
                                this_name = list(set(rackwise_nodenames).intersection(other_names) )
                                if len(this_name):

                                    value = [ this_name[0], ]
                                    good = True

                                else:

                                    for r in rackwise_nodenames:
                                        logging.debug( "rackwise name... %s" % (r,))

                                        # range of names
                                        m = search( '^(?P<name>\D+)(?P<fm>\d{3,})\-(?P<to>\d{3,})$', str(r) )
                                        ranges = []
                                        if m:
                                            d = m.groupdict()
                                            # same leading digits
                                            ranges = [ "%s%s"%(d['name'],str(n).zfill(len(d['fm']))) for n in xrange( int(d['fm']), int(d['to'])+1 ) ]
                                        elif match( r'\/ \S+$', str(r) ):
                                            logging.error( 'BULLET NAME!')
                                        else:
                                            # array of names
                                            ranges = compile(r'\s*(,|;|\n)\s*').split(r)
                                            # logging.error("RANGE %s" % (ranges,))

                                        # inside?
                                        logging.debug("rackwise nodename range: %s -> %s of %s" % (r,ranges,other_names))
                                        this_name = list(set(other_names).intersection(ranges) )
                                        if len(this_name):
                                            logging.debug(' nodename checks out okay in range of hosts in rackwise')
                                            value = [ this_name[0], ]
                                        
                                            good = True
                                        
                                        # just do plain text search
                                        elif len(rackwise_nodenames) == 1:
                                            logging.debug("plain text rackwise string search %s" % (this,))
                                            for o in other_names:
                                                if o in rackwise_nodenames[0]:
                                                    value = [o,]
                                                    good = True
                                        # delete rackwise entry
                                        # if good:
                                        #     logging.error("DEL %s" % )
                                        
                    
                        # elif g == 'port':
                            # logging.error("HERE HERE %s" % (this,))
                        #     if isinstance( this, dict ) and len( this ) > 1:
                        #         logging.error("ERROR HERE")
                    
                        if not good:
                            logging.debug(" not good for some reason: %s" % (this,))
                            ok = False
                            # bad[g] = s
                            # logging.error("BAD: %s\t%s" % (g,summary[g]['data'],))
                            if not g in bad:
                                bad[g] = {}
                            for k,b in summary[g]['data'].iteritems():
                                # logging.debug("  g: %s\tk: %s\tb: %s" % (g,k,b))
                                if not k in bad[g]:
                                    bad[g][k] = {}

                                for v,a in b.iteritems():
                                    # logging.debug("    g: %s, k: %s, v: %s\ta: %s" % (g,k,v,a))
                                    if not v in bad[g][k]:
                                        bad[g][k][v] = []
                                    # logging.error("    add: %s" % a)
                                    for c in a:
                                        if not c in bad[g][k][v]:
                                            bad[g][k][v].append(c)
                                            logging.debug("  setting as bad: %s:%s value \'%s\' source %s" % (g,k,v,c,) )
                            # logging.error("BAD: %s" % (bad,))
                    # logging.error("ADDING %s" % (bad,))
                    state.append( ok )
                except TypeError,e:
                    # if e:
                    #     logging.error("type error: %s" % e)
                    value = None
                except KeyError,e:
                    # logging.warn("No key %s for field %s on %s" % (e,f,summary[g]['data'] ))
                    value = None
                logging.debug(" + summarised field %s\tas %s" % (f,value))
                if isinstance(value,list):
                    c = len(value)
                    # if c == 1:
                    #     out[f] = value[0]
                    if c == 0:
                        out[f] = None
                    # elif c > 1:
                    else:
                        out[f] = value
                else:
                    out[f] = value
                    
            # logging.info("OUT: %s" % (out,))
            yield out, \
                state, \
                "%s"%i['strategy'] if 'strategy' in i else null_char, \
                bad, \
                datasources(i), \
                i, \
                ignore_done


def report_item( db, search, 
        fields=(), 
        subnets={}, 
        order_by=('nodename.value',ASCENDING), 
        unique_fields=( 'port:ip_address', 'port:mac_address', 'port:hostname', 'port:dhcp', 'subnet' ),
        ignore_fields=(),
        max_value_fields=( 'updated_at', ),
        multivalue_fields=( 'admin_group', '_ports_', 'admin:username', 'admin:id', 'admin:lastname' ) ):
    # yield unique device which may have multiple network interface ports
    
    seen = {}
    logging.debug( "-----------" )
    logging.debug( "reporting %s" % (search,) )
    local_errors = {}   # errors for just singel entry
    data = []
    
    nodenames = []
    
    already_processed = 0
    
    last_seen = None
    
    for item, state, strategy, errors, sources, d, ignore_done in collate( db.find( search ), order_by, fields=fields, subnets=subnets ):

        logging.debug( " [%s] %s" % ( 'X' if 'done' in d else 'O', item,) )
        data.append( d )
        
        if errors:
            logging.debug("local err: %s" % (errors,))

        # as long as it looks good...
        if ignore_done or not 'done' in d:

            logging.debug("ITEM 1: %s" % (item,))

            # assume the search worked to link related records, so use name
            # logging.error("NODENAME: %s \t of %s" % (item['nodename'],nodenames))
            if item['nodename']:
                for n in item['nodename']:
                    if not n in nodenames:
                        nodenames.append( n )
                
            elif not item['nodename']:
                if len(nodenames):
                    item['nodename'] = nodenames
                else:
                    # none for nodename - eg devices not seen by central systems
                    # logging.error("NO NODENAME! for %s" % (item,))
                    item['nodename'] = [ None, ]

            for n in item['nodename']:
                # logging.error("THISN: %s" % (n,))
                if not n in seen:
                    seen[n] = {}
                    for f in multivalue_fields:
                        seen[n][f] = []
                    for f in item:
                        if not f in unique_fields:
                            seen[n][f] = []

                for k,v in item.iteritems():
                            
                    if not k in unique_fields:
                        # if v and not v in seen[n][k]:
                        if v:
                            for x in v:
                                if not x in seen[n][k]:
                                    logging.debug( '  setting %s\t%s \t %s' % (n,k,x))
                                    seen[n][k].append(x)

                for a in determine_admin_group( item ):
                    if not a in seen[n]['admin_group']:
                        seen[n]['admin_group'].append( a )

                port = {}
                # logging.debug("ITEM 2: %s" % (item,))
                for f in unique_fields:
                    # logging.debug( '  port: %s\t%s' % (f,item[f]))
                    if f in item:
                        port[f] = item[f]
                    else:
                        port[f] = None
                # double check that only single values exist for ports

                # for k,v in port.iteritems():
                #     logging.error("PORT: %s %s" % (k,v))
                #     if isinstance(v,list) and len(v) > 1:
                #         pass

                logging.debug('  adding _port_ %s' % (port,))
                seen[n]['_ports_'].append(port)

                for f in max_value_fields:
                    # logging.error("MAX: %s" % (seen[n][f],))
                    if len( seen[n][f] ):
                        dt = max(seen[n][f])
                        seen[n][f] = [dt,]
            
            # mark in db that this has been used
            d['done'] = True
            db.save( d )
            
        else:
            logging.debug("already processed: %s" % (item,))
            already_processed = already_processed + 1

        # aggregate multiple errors together from each
        if errors:
            logging.debug('aggregating local errors')
            for k,b in errors.iteritems():
                # logging.debug(" multi value error: k=%s\tb=%s" % (k,b))
                if not k in local_errors:
                    local_errors[k] = {}
                for subject,c in b.iteritems():
                    # logging.debug("  k=%s\tv=%s\ta=%s" % (k,subject,a))
                    if not subject in local_errors[k]:
                        local_errors[k][subject] = {}
                    for v,f in c.iteritems():
                        if not v in local_errors[k][subject]:
                            local_errors[k][subject][v] = []
                        local_errors[k][subject][v] = []
                        for x in f:
                            if not x in local_errors[k][subject][v]:
                                # logging.debug( "VTYPE: %s %s" % (v,type(v)))
                                logging.debug("  adding local error: source %s\tto %s:%s\tvalue %s" % (x,k,subject,v))
                                local_errors[k][subject][v].append(x)
            logging.debug("local errors: %s" % (local_errors,))
        else:
            logging.debug('no local errors')

    if already_processed > 0:
        logging.debug('  already processed: %s' % (already_processed,))

    # flatten and yield for each unique nodename
    for n,node in seen.iteritems():

        this_node = {}
        group_errors = {}  # errors for this grouping of entries for this node
        
        logging.debug("reporting node: %s \t%s" % (node['nodename'], node,))
        processed_ports = []
        
        for field,values in node.iteritems():
            
            # collapse
            # logging.error("  K: %s\tV: %s" % (k,v))
            if not field in multivalue_fields:
                if len(values) > 1:
                    # set values
                    this_node[field] = [ this_node[field], ] if field in this_node else []
                    for val in values:
                        if not val in this_node[field]:
                            this_node[field].append( val )

                    construct_group_error( group_errors, field, values, data )

                else:
                    logging.debug(" no group errors with %s\t%s" % (field,values))
                    if len(values) == 1:
                        this_node[field] = values[0]
                        try: # remove stupid chars if string
                            this_node[field] = sub( r'\n', ' ', this_node[field] )
                        except:
                            pass
                    elif len(values) == 0:
                        this_node[field] = None

            else:
                # logging.error("FIELD 1: %s \t%s"%(field,node[field]) )
                this_node[field] = node[field]
                # logging.error("FIELD 2: %s \t%s"%(field,this_node[field]) )

            if field == '_ports_':
                # report on bad ports
                for f, v in port.iteritems():
                    if v == None:
                        pass
                    elif len(v) > 1:
                        construct_group_error( group_errors, f, v, data )

                # determine uniq ports on device
                uniq = {}
                for port in values:
                    # if it fails, add upstream
                    add = True
                    try:
                        # logging.info("PORT %s" % (port,))
                        p = flatten(port)
                        s = stringify( p )
                        if not s in uniq:
                            uniq[s] = 0
                        else:
                            add = False
                        uniq[s] = uniq[s] + 1
                    except:
                        pass
                    finally:
                        if add:
                            processed_ports.append(port)

        # set final
        this_node['_ports_'] = processed_ports

        logging.debug("reporting: %s" % (this_node,))

        # if not nodename, then use the pc number if exists
        # logging.error("NAME: %s" % (this_node['nodename'],))
        if not this_node['nodename']:
            if 'PC' in this_node and this_node['PC']:
                this_node['nodename'] = this_node['PC']
            else:
                this_node['nodename'] = 'unk-%s'%(this_node['_ports_'][0]['port:mac_address'][0].replace(':',''),)
                # not_slac = not_slac + 1
        # logging.error(" -> NAME: %s" % (this_node['nodename'],))
        
        
        if not this_node['device_type']:
            this_node['device_type'] = 'computer'
        
        
        # logging.error("LOCAL ERRORS: %s" % (local_errors,))
        # logging.error("GLOBAL ERRORS: %s" % (group_errors,))
        
        if len(group_errors) > 0:

            # let's reduce common issues
            clean_error('owner','value','SLAC', group_errors, this_node)
            clean_error('device_type','value','computer', group_errors, this_node)
            clean_error('device_type','value','server', group_errors, this_node)
            clean_error('device_type','value','switch', group_errors, this_node)

            # remove_dhcp_errors('user',None,None, group_errors, this_node)
            remove_source_errors('dhcp', 'user',None, group_errors, this_node)
            remove_source_errors('cando', 'model', None, group_errors, this_node)
            remove_source_errors('cando', 'location', None, group_errors, this_node)
            remove_source_errors('cando', 'user', None, group_errors, this_node)
            # remove_source_errors('cando', 'os', 'name', group_errors, this_node)

            # trust ptolemy device models
            take_source( 'ptolemy_device', 'model', None, group_errors, this_node )

            go_with_popular('location','room', None, group_errors, this_node)

            if 'device_type' in group_errors:
                vals = group_errors['device_type']['value'].keys()
                if 'laptop' in vals and 'unix server' in vals:
                    del group_errors['device_type']['value']['unix server']
                if len( group_errors['device_type']['value'].keys() ) == 1:
                    del group_errors['device_type']


        if len(local_errors) > 0:

            # deal with stupid device types
            if 'device_type' in local_errors:
                vals = local_errors['device_type']['value'].keys()
                if len(vals) > 1 and 'computer' in vals:
                    del local_errors['device_type']['value']['computer']
                if ('unix server' in vals or 'windows server' in vals) and 'server' in vals:
                    del local_errors['device_type']['value']['server']
                if ( 'router' in vals ) and 'switch' in vals:
                    del local_errors['device_type']['value']['switch']
                if 'laptop' in vals and 'unix server' in vals:
                    del local_errors['device_type']['value']['unix server']
                if 'storage server' in vals and 'server' in vals:
                    del local_errors['device_type']['value']['server']
                if ( 'server' in vals ) and 'router' in vals:
                    del local_errors['device_type']['value']['router']
                    
                if len( local_errors['device_type']['value'].keys() ) == 1:
                    del local_errors['device_type']
                # logging.error("HERE %s\t%s" % (local_errors,this_node))
                        
            take_source( 'ptolemy_device', 'model', None, local_errors, this_node )
        
        # determine if we have same mac address and or same ip addresses repeated for this node        
        if len(local_errors) > 0:
            logging.debug("LOCAL ERRORS %s" % (local_errors,))
            yield False, this_node, local_errors, 'local'

        elif len(group_errors) > 0:
            logging.debug("GROUP ERRORS %s" % (group_errors,))
            yield False, this_node, group_errors, 'group'

        else:
            logging.debug("NO ERRORS")
            yield True, this_node, None, None

def construct_group_error( group_errors, field, values, data ):
    logging.debug(" aggregating group errors with %s\t%s" % (field,values))
    s,t = split_field( field )
    if not s in group_errors:
        group_errors[s] = {}
    if not t in group_errors[s]:
        group_errors[s][t] = {}
    for val in values:
        if not val in group_errors[s][t]:
            group_errors[s][t][val] = []
        # logging.debug("DATA: %s" % (data,))
        for d in data:
            if s in d:
                for i in d[s]:
                    if t in i and i[t] == val:
                        # logging.debug("> %s in db %s" % (i[t],i['db']))
                        if not i['db'] in group_errors[s][t][val]:
                            logging.debug('  adding group error: source %s\t%s:%s\t%s' % (i['db'],s,t,val))
                            group_errors[s][t][val].append( i['db'] )



def del_errors( to_delete, k, x, errors, node ):
    for n in to_delete:
        try:
            logging.debug("  deleting %s %s %s" % (k,x,n,))
            del errors[k][x][n]
            if len(errors[k][x]) == 1:
                del errors[k][x]
        except:
            pass
        key = '%s%s'%(k,':%s'%x if not x == 'value' else '')
        if isinstance( node[key], list):
            node[key].remove(n)
            if len(node[key]) == 1:
                node[key] = node[key].pop(0)

def remove_source_errors( source , k,s, errors, node):
    """ remove entries if other values exist for source k """
    if k in errors:
        # logging.error("INSIDE %s" % (errors,))
        # determine if dhcp is part
        for x in errors[k].keys():
            y = errors[k][x]
            # logging.error("X: %s, Y: %s" % (x,y))
            to_delete = []
            for z,values in y.iteritems():
                if source in values and len(values) == 1:
                    to_delete.append(z)
            del_errors( to_delete, k, x, errors, node )
        if len(errors[k]) == 0:
            del errors[k]

def take_source( source , k,s, errors, node):
    """ remove entries if other values exist for source k """
    if k in errors:
        # logging.error("INSIDE %s" % (errors,))
        # determine if dhcp is part
        for x in errors[k].keys():
            y = errors[k][x]
            # logging.debug("X: %s, Y: %s" % (x,y))
            to_delete = []
            for z,values in y.iteritems():
                if not source in values:
                    to_delete.append(z)
            del_errors( to_delete, k, x, errors, node )
        if len(errors[k]) == 0:
            del errors[k]
                    
def clean_error( k,s,v, errors, node ):
    # logging.debug("cleaning %s %s %s:\t%s" % (k,s,v,errors))
    if k in errors and s in errors[k] and errors[k][s] > 1 and v in errors[k][s]:
        logging.debug('removing %s %s = %s' % (k,s,v) )
        # clean up errors
        del errors[k][s][v]
        if len( errors[k][s] ) == 1:
            del errors[k]
        key = '%s%s'%(k,':%s'%s if not s == 'value' else '')
        # clean up node
        # logging.error("KEY: %s\t%s = %s" % (key,node,v))
        node[key].remove(v)
        if len(node[key]) == 1:
            node[key] = node[key].pop(0)

def go_with_popular( k,s,v, errors, node ):
    # logging.error("HERE %s %s %s: %s" % (k,s,v,errors))
    if k in errors and s in errors[k] and errors[k][s] > 1:
        keep_value = None
        all_values = []
        c = 0
        for v in errors[k][s]:
            all_values.append(v)
            if len( errors[k][s][v] ) > c:
                keep_value = v
        # logging.error("KEEP: %s %s %s %s" % (k,s,keep_value,all_values))
        for a in all_values:
            if not a == keep_value:
                clean_error( k,s,a, errors, node )

def report( mongo, clear_states=True, process_main=True, process_others=False, **kwargs ):
    """ return unique nodes; nodes with valid multiple interfaces will have len(_ports_) > 1 """
    # copy db over
    db = get_mongo_collection( mongo, kwargs['db_name'] )

    # remove all 'done' fields
    if clear_states:
        logging.debug("clearing done states")
        db.update( { 'done': True }, { '$unset': { 'done': False } }, upsert=False, multi=True )

    order_by = ('nodename',ASCENDING)

    # go through list twice to determine unique real nodenames
    nodenames = {}
    search = {}
    if len(kwargs['nodenames']) == 0:
        process_others = True
        nodenames = {}
    else:
        search = { 'nodename.value': kwargs['nodenames'].pop(0) }
        nodenames = {}

    logging.info("searching for %s" % (search,))

    # get pc numbers to do match also
    for i in db.find( search, { 'nodename.value': 1, 'PC.value': 1, '_id': 0 } ):
        logging.debug("%s " % (i,))
        if 'nodename' in i:
            for node in i['nodename']:
                v = node['value']
                if not ( ';' in v or '/' in v or '#' in v ):
                    if not v in nodenames:
                        nodenames[v] = []
                    if 'PC' in i:
                        for p in i['PC']:
                            pc = p['value']
                            if not pc in nodenames[v]:
                                nodenames[v].append(pc)
    # logging.debug("NODENAMES: %s" % (nodenames,))

    if process_main:
        for n in nodenames:
            # ignore special case of null hostnames til later
            if n == None:
                continue
            search = [ { 'nodename.value': n } ]
            if isinstance( nodenames, dict ):
                # ignore none's
                for pc in nodenames[n]:
                    if pc:
                        search.append( { 'PC.value': pc } )
            # logging.error("SEARCH: %s" % (search,))
            for a,b,c,d in report_item( db, { '$or': search }, fields=kwargs['fields'], subnets=kwargs['subnets'], order_by=order_by ):
                yield a,b,c,d

    # deal with all not yet processes (ie those without a hostname)
    if process_others:
        others = {}
        for i in db.find( { '$and': [ { 'nodename.value': None }, { 'done': None } ] } ):
            # logging.error("I: %s" % (i,))
            for p in i['port']:
                others[ p['mac_address'] ] = True
        # logging.error("MAC: %s" % (others,))
        for m in others:
            for a,b,c,d in report_item( db, { 'port.mac_address': m }, fields=kwargs['fields'], subnets=kwargs['subnets'], order_by=order_by ):
                yield a,b,c,d
                
    # do a post scan? ie distinct( 'port.hostname' )?
    # for n,i in enumerate(db.find( { 'done': None } )):
    #     logging.error("LEFT NOT DONE: %s\t%s " % (n,i))

def format_errors( errors, order=( 'serial', 'PC', 'nodename', 'manufacturer', 'model', 'PO', 'admin', 'user', ) ):
    def give( k,v ):
        for x,y in v.iteritems():
            k_prime = '%s%s' % (k,'' if x == 'value' else ':%s' % x)
            # logging.error("  x: %s\tk: %s\ty: %s" % (x,k_prime,y))
            yield '*%s*: %s' %(k_prime,y)
    if errors:
        for o in order:
            if o in errors:
                for i in give( o,errors[o] ):
                    yield i
                    # logging.error("ERRORS %s: %s"%(o,errors,))
                    if o in errors:
                        del errors[o]
        for k in errors:
            for i in give( k,errors[k] ):
                yield i


def flatten( d ):
    out = {}
    for k,v in d.iteritems():
        out[k] = v
        if v:
            if len(v) == 1:
                out[k] = v[0]
            else:
                raise Exception('error in data %s' % (v,))
    return out
    