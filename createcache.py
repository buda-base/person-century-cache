import io
import os
import yaml
import json
from pathlib import Path
import glob
from rdflib import URIRef, Literal, BNode, Graph, ConjunctiveGraph
from rdflib.namespace import RDF, RDFS, SKOS, OWL, Namespace, NamespaceManager, XSD
import requests
from tqdm import tqdm
import sys
from datetime import datetime
import time
import re
import hashlib

BASE_MAX_DIM=370
BASE_CROP_DIM=185
MAX_RATIO=2

GITPATH = "../xmltoldmigration/tbrc-ttl/persons/"
if len(sys.argv) > 1:
    GITPATH = sys.argv[1]

VERBMODE = "-v"
if len(sys.argv) > 2:
    VERBMODE = sys.argv[2]

BDR = Namespace("http://purl.bdrc.io/resource/")
BDO = Namespace("http://purl.bdrc.io/ontology/core/")
TMP = Namespace("http://purl.bdrc.io/ontology/tmp/")
BDA = Namespace("http://purl.bdrc.io/admindata/")
ADM = Namespace("http://purl.bdrc.io/ontology/admin/")

NSM = NamespaceManager(Graph())
NSM.bind("bdr", BDR)
NSM.bind("bdo", BDO)
NSM.bind("tmp", TMP)
NSM.bind("bda", BDA)
NSM.bind("adm", ADM)
NSM.bind("skos", SKOS)
NSM.bind("rdfs", RDFS)

def getsimpledates(person, model):
    taq = 9999
    tpq = 0
    for _, _, e in model.triples((person, BDO.personEvent, None)):
        eType = None
        
        for _, _, t in model.triples((e, RDF.type, None)):
            eType = t
        for _, p, o in model.triples((e, None, None)):
            if p in [BDO.onYear, BDO.notBefore, BDO.notAfter, BDO.onDate]:
                yearstr = str(o)
                if len(yearstr) > 4:
                    yearstr = yearstr[:4]
                year = 0
                try:
                    year = int(yearstr)
                except:
                    continue
                if eType == BDO.PersonBirth:
                    # case of P2008
                    if p != BDO.notAfter or not yearstr.endswith("99"):
                        year += 15
                if taq > year:
                    taq = year
                if tpq < year:
                    tpq = year
    return (taq, tpq)


def getlinks(person, model):
    res = []
    for _, p, o in model.triples((person, None, None)):
        if p == RDF.type:
            continue
        if type(o) is URIRef:
            _, _, oLname = NSM.compute_qname_strict(o)
            if oLname.startswith("P"):
                _, _, pLname = NSM.compute_qname_strict(p)
                res.append((oLname, pLname))
    return res

def getcenturyfordates(taq, tpq, kb, personLname):
    firstc = taq//100
    secondc = tpq//100
    res = []
    diff = secondc == firstc
    if diff == 0:
        res.append(firstc+1)
        return res
    if diff > 1 and diff < 4:
        for i in range(firstc, secondc+1):
            res.append(i+1)
        return res
    if diff > 3:
        if "problematic" not in kb:
            kb["problematic"] = []
        kb["problematic"].append(personLname)
        res.append(secondc)
        return res
    if secondc-firstc == 1:
        yearsinfirstc = (100*secondc)-taq
        yearsinsecondc = tpq-(100*secondc)
        if yearsinsecondc == 0:
            res.append(firstc+1)
            return res
        ratio = yearsinfirstc/yearsinsecondc
        if ratio < 0.2:
            res.append(secondc+1)
            return res
        if ratio > 5:
            res.append(firstc+1)
            return res
        res.append(secondc+1)
        res.append(firstc+1)
        return res
    return res

def cacheforPfile(pFilePath, res, kb):
    # if file name is the same as an image instance already present in the database, don't read file:
    #likelypQname = "bdr:"+Path(pFilePath).stem
    model = ConjunctiveGraph()
    model.parse(str(pFilePath), format="trig")
    if (None,  ADM.status, BDA.StatusReleased) not in model:
        return None
    found = False
    for person, _, _ in model.triples((None, RDF.type, BDO.Person)):
        dates = getsimpledates(person, model)
        _, _, personLname = NSM.compute_qname_strict(person)
        if dates[0] == 9999:
            links = getlinks(person, model)
            kb[personLname] = {"links": links}
            return False
        kb[personLname] = {"dates": dates}
        centuries = getcenturyfordates(dates[0], dates[1], kb, personLname)
        for c in centuries:
            if c not in kb["percentury"]:
                kb["percentury"][c] = 0
            kb["percentury"][c] += 1
            res.add((person, TMP.associatedCentury, Literal(c, datatype=XSD.integer)))
            found = True
    return found

def addmissing(res, kb):
    i = 0
    for p, info in kb.items():
        if not p.startswith("P"):
            continue
        if "dates" in info or "links" not in info:
            continue
        Iadd = 0
        for l in info["links"]:
            linkedP = l[0]
            if linkedP not in kb or "dates" not in kb[linkedP]:
                continue
            dates = kb[linkedP]["dates"]
            linkedProp = l[1]
            dateshift = 0
            if linkedProp in ["hasMother", "hasFather", "hasParent", "hasAunt", "hasUncle", "personStudentOf"]:
                dateshift = -20
            if linkedProp in ["isIncarnation", "incarnationYangtse", "incarnationBody", "incarnationGeneral", "incarnationMind", "incarnationQualities", "incarnationSpeech", "incarnationYangtse"]:
                dates = (dates[1], dates[1]+30)
            if linkedProp in ["hasChild", "hasSon", "hasDaughter", "hasNiece", "hasNephew", "personTeacherOf"]:
                dateshift = 20
            if linkedProp in ["hasIncarnation", "hasIncarnationActivities", "hasIncarnationBody", "hasIncarnationGeneral", "hasIncarnationMind", "hasIncarnationQualities", "hasIncarnationSpeech", "hasIncarnationYangtse"]:
                dates = (dates[0]-30, dates[0])
            centuries = getcenturyfordates(dates[0]+dateshift, dates[1]+dateshift, kb, p)
            for c in centuries:
                if c not in kb["percentury"]:
                    kb["percentury"][c] = 0
                kb["percentury"][c] += 1
                res.add((BDR[p], TMP.associatedCentury, Literal(c, datatype=XSD.integer)))
            Iadd = 1
        i += Iadd
    return i

def main(wrid=None):
    res = Graph()
    NSM = NamespaceManager(res)
    NSM.bind("bdr", BDR)
    NSM.bind("bdo", BDO)
    NSM.bind("xsd", XSD)
    NSM.bind("tmp", TMP)
    kb={"percentury": {}}
    if wrid is not None:
        md5 = hashlib.md5(str.encode(wrid))
        two = md5.hexdigest()[:2]
        cacheforPfile(GITPATH+'/'+two+'/'+wrid+'.trig', res, kb)
        print(yaml.dump(kb))
        return
    l = sorted(glob.glob(GITPATH+'/**/P*.trig'))
    nbFound = 0
    nbNotFound = 0
    for fname in VERBMODE == "-v" and tqdm(l) or l:
        found = cacheforPfile(fname, res, kb)
        if found is True:
            nbFound += 1
            #break
        elif found is False:
            nbNotFound += 1
            #break
    nbInferred = addmissing(res, kb)
    res.serialize("centuries.ttl", format="turtle")
    print("found %d, inferred %d, no info on %d" % (nbFound, nbInferred, nbNotFound-nbInferred))
    print(kb["percentury"])
    with open("kb.yml", 'w') as stream:
        yaml.dump(kb, stream)

def testgetc():
    print(getcenturyfordates(1550, 1590, {}, ""))
    print(getcenturyfordates(1550, 1600, {}, ""))
    print(getcenturyfordates(1550, 1603, {}, ""))
    print(getcenturyfordates(1550, 1650, {}, ""))
    print(getcenturyfordates(1250, 1650, {}, ""))
    print(getcenturyfordates(1500, 1599, {}, ""))
    print(getcenturyfordates(1700, 1899, {}, ""))

#testgetc()
main("P2008")
main()