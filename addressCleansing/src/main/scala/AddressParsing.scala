package main.scala

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{expr, udf, when}

import scala.util.matching.Regex

object AddressParsing {

  var streetTypeMap: scala.collection.mutable.HashMap[String, String] = scala.collection.mutable.HashMap()

  var directionalsMap: scala.collection.mutable.HashMap[String, String] = scala.collection.mutable.HashMap()


  directionalsMap.put("NORTHEAST", "NE")
  directionalsMap.put("SOUTHEAST", "SE")
  directionalsMap.put("SOUTHWEST", "SW")
  directionalsMap.put("NORTHWEST", "NW")
  directionalsMap.put("NORTH", "N")
  directionalsMap.put("EAST", "E")
  directionalsMap.put("SOUTH", "S")
  directionalsMap.put("WEST", "W")

  streetTypeMap.put("ALEE", "ALY")
  streetTypeMap.put("ALLEY", "ALY")
  streetTypeMap.put("ALLY", "ALY")
  streetTypeMap.put("ANEX", "ANX")
  streetTypeMap.put("ANNEX", "ANX")
  streetTypeMap.put("ANNX", "ANX")
  streetTypeMap.put("ARCADE", "ARC")
  streetTypeMap.put("AV", "AVE")
  streetTypeMap.put("AVEN", "AVE")
  streetTypeMap.put("AVENU", "AVE")
  streetTypeMap.put("AVENUE", "AVE")
  streetTypeMap.put("AVN", "AVE")
  streetTypeMap.put("AVNUE", "AVE")
  streetTypeMap.put("BAYOO", "BYU")
  streetTypeMap.put("BAYOU", "BYU")
  streetTypeMap.put("BEACH", "BCH")
  streetTypeMap.put("BEND", "BND")
  streetTypeMap.put("BLUF", "BLF")
  streetTypeMap.put("BLUFF", "BLF")
  streetTypeMap.put("BLUFFS", "BLFS")
  streetTypeMap.put("BOT", "BTM")
  streetTypeMap.put("BOTTM", "BTM")
  streetTypeMap.put("BOTTOM", "BTM")
  streetTypeMap.put("BOUL", "BLVD")
  streetTypeMap.put("BOULEVARD", "BLVD")
  streetTypeMap.put("BOULV", "BLVD")
  streetTypeMap.put("BRANCH", "BR")
  streetTypeMap.put("BRDGE", "BRG")
  streetTypeMap.put("BRIDGE", "BRG")
  streetTypeMap.put("BRNCH", "BR")
  streetTypeMap.put("BROOK", "BRK")
  streetTypeMap.put("BROOKS", "BRKS")
  streetTypeMap.put("BURG", "BG")
  streetTypeMap.put("BURGS", "BGS")
  streetTypeMap.put("BYPA", "BYP")
  streetTypeMap.put("BYPAS", "BYP")
  streetTypeMap.put("BYPASS", "BYP")
  streetTypeMap.put("BYPS", "BYP")
  streetTypeMap.put("CAMP", "CP")
  streetTypeMap.put("CANYN", "CYN")
  streetTypeMap.put("CANYON", "CYN")
  streetTypeMap.put("CAPE", "CPE")
  streetTypeMap.put("CAUSEWAY", "CSWY")
  streetTypeMap.put("CAUSWAY", "CSWY")
  streetTypeMap.put("CEN", "CTR")
  streetTypeMap.put("CENT", "CTR")
  streetTypeMap.put("CENTER", "CTR")
  streetTypeMap.put("CENTERS", "CTRS")
  streetTypeMap.put("CENTR", "CTR")
  streetTypeMap.put("CENTRE", "CTR")
  streetTypeMap.put("CIRC", "CIR")
  streetTypeMap.put("CIRCL", "CIR")
  streetTypeMap.put("CIRCLE", "CIR")
  streetTypeMap.put("CIRCLES", "CIRS")
  streetTypeMap.put("CK", "CRK")
  streetTypeMap.put("CLIFF", "CLF")
  streetTypeMap.put("CLIFFS", "CLFS")
  streetTypeMap.put("CLUB", "CLB")
  streetTypeMap.put("CMP", "CP")
  streetTypeMap.put("CNTER", "CTR")
  streetTypeMap.put("CNTR", "CTR")
  streetTypeMap.put("CNYN", "CYN")
  streetTypeMap.put("COMMON", "CMN")
  streetTypeMap.put("CORNER", "COR")
  streetTypeMap.put("CORNERS", "CORS")
  streetTypeMap.put("COURSE", "CRSE")
  streetTypeMap.put("COURT", "CT")
  streetTypeMap.put("COURTS", "CTS")
  streetTypeMap.put("COVE", "CV")
  streetTypeMap.put("COVES", "CVS")
  streetTypeMap.put("CR", "CRK")
  streetTypeMap.put("CRCL", "CIR")
  streetTypeMap.put("CRCLE", "CIR")
  streetTypeMap.put("CRECENT", "CRES")
  streetTypeMap.put("CREEK", "CRK")
  streetTypeMap.put("CRESCENT", "CRES")
  streetTypeMap.put("CRESENT", "CRES")
  streetTypeMap.put("CREST", "CRST")
  streetTypeMap.put("CROSSING", "XING")
  streetTypeMap.put("CROSSROAD", "XRD")
  streetTypeMap.put("CRSCNT", "CRES")
  streetTypeMap.put("CRSENT", "CRES")
  streetTypeMap.put("CRSNT", "CRES")
  streetTypeMap.put("CRSSING", "XING")
  streetTypeMap.put("CRSSNG", "XING")
  streetTypeMap.put("CRT", "CT")
  streetTypeMap.put("CURVE", "CURV")
  streetTypeMap.put("DALE", "DL")
  streetTypeMap.put("DAM", "DM")
  streetTypeMap.put("DIV", "DV")
  streetTypeMap.put("DIVIDE", "DV")
  streetTypeMap.put("DRIV", "DR")
  streetTypeMap.put("DRIVE", "DR")
  streetTypeMap.put("DRIVES", "DRS")
  streetTypeMap.put("DRV", "DR")
  streetTypeMap.put("DVD", "DV")
  streetTypeMap.put("ESTATE", "EST")
  streetTypeMap.put("ESTATES", "ESTS")
  streetTypeMap.put("EXP", "EXPY")
  streetTypeMap.put("EXPR", "EXPY")
  streetTypeMap.put("EXPRESS", "EXPY")
  streetTypeMap.put("EXPRESSWAY", "EXPY")
  streetTypeMap.put("EXPW", "EXPY")
  streetTypeMap.put("EXTENSION", "EXT")
  streetTypeMap.put("EXTENSIONS", "EXTS")
  streetTypeMap.put("EXTN", "EXT")
  streetTypeMap.put("EXTNSN", "EXT")
  streetTypeMap.put("FALLS", "FLS")
  streetTypeMap.put("FERRY", "FRY")
  streetTypeMap.put("FIELD", "FLD")
  streetTypeMap.put("FIELDS", "FLDS")
  streetTypeMap.put("FLAT", "FLT")
  streetTypeMap.put("FLATS", "FLTS")
  streetTypeMap.put("FORD", "FRD")
  streetTypeMap.put("FORDS", "FRDS")
  streetTypeMap.put("FOREST", "FRST")
  streetTypeMap.put("FORESTS", "FRST")
  streetTypeMap.put("FORG", "FRG")
  streetTypeMap.put("FORGE", "FRG")
  streetTypeMap.put("FORGES", "FRGS")
  streetTypeMap.put("FORK", "FRK")
  streetTypeMap.put("FORKS", "FRKS")
  streetTypeMap.put("FORT", "FT")
  streetTypeMap.put("FREEWAY", "FWY")
  streetTypeMap.put("FREEWY", "FWY")
  streetTypeMap.put("FRRY", "FRY")
  streetTypeMap.put("FRT", "FT")
  streetTypeMap.put("FRWAY", "FWY")
  streetTypeMap.put("FRWY", "FWY")
  streetTypeMap.put("GARDEN", "GDN")
  streetTypeMap.put("GARDENS", "GDNS")
  streetTypeMap.put("GARDN", "GDN")
  streetTypeMap.put("GATEWAY", "GTWY")
  streetTypeMap.put("GATEWY", "GTWY")
  streetTypeMap.put("GATWAY", "GTWY")
  streetTypeMap.put("GLEN", "GLN")
  streetTypeMap.put("GLENS", "GLNS")
  streetTypeMap.put("GRDEN", "GDN")
  streetTypeMap.put("GRDN", "GDN")
  streetTypeMap.put("GRDNS", "GDNS")
  streetTypeMap.put("GREEN", "GRN")
  streetTypeMap.put("GREENS", "GRNS")
  streetTypeMap.put("GROV", "GRV")
  streetTypeMap.put("GROVE", "GRV")
  streetTypeMap.put("GROVES", "GRVS")
  streetTypeMap.put("GTWAY", "GTWY")
  streetTypeMap.put("HARB", "HBR")
  streetTypeMap.put("HARBOR", "HBR")
  streetTypeMap.put("HARBORS", "HBRS")
  streetTypeMap.put("HARBR", "HBR")
  streetTypeMap.put("HAVEN", "HVN")
  streetTypeMap.put("HAVN", "HVN")
  streetTypeMap.put("HEIGHT", "HTS")
  streetTypeMap.put("HEIGHTS", "HTS")
  streetTypeMap.put("HGTS", "HTS")
  streetTypeMap.put("HIGHWAY", "HWY")
  streetTypeMap.put("HIGHWY", "HWY")
  streetTypeMap.put("HILL", "HL")
  streetTypeMap.put("HILLS", "HLS")
  streetTypeMap.put("HIWAY", "HWY")
  streetTypeMap.put("HIWY", "HWY")
  streetTypeMap.put("HLLW", "HOLW")
  streetTypeMap.put("HOLLOW", "HOLW")
  streetTypeMap.put("HOLLOWS", "HOLW")
  streetTypeMap.put("HOLWS", "HOLW")
  streetTypeMap.put("HRBOR", "HBR")
  streetTypeMap.put("HT", "HTS")
  streetTypeMap.put("HWAY", "HWY")
  streetTypeMap.put("INLET", "INLT")
  streetTypeMap.put("ISLAND", "IS")
  streetTypeMap.put("ISLANDS", "ISS")
  streetTypeMap.put("ISLES", "ISLE")
  streetTypeMap.put("ISLND", "IS")
  streetTypeMap.put("ISLNDS", "ISS")
  streetTypeMap.put("JCTION", "JCT")
  streetTypeMap.put("JCTN", "JCT")
  streetTypeMap.put("JCTNS", "JCTS")
  streetTypeMap.put("JUNCTION", "JCT")
  streetTypeMap.put("JUNCTIONS", "JCTS")
  streetTypeMap.put("JUNCTN", "JCT")
  streetTypeMap.put("JUNCTON", "JCT")
  streetTypeMap.put("KEY", "KY")
  streetTypeMap.put("KEYS", "KYS")
  streetTypeMap.put("KNOL", "KNL")
  streetTypeMap.put("KNOLL", "KNL")
  streetTypeMap.put("KNOLLS", "KNLS")
  streetTypeMap.put("LA", "LN")
  streetTypeMap.put("LAKE", "LK")
  streetTypeMap.put("LAKES", "LKS")
  streetTypeMap.put("LANDING", "LNDG")
  streetTypeMap.put("LANE", "LN")
  streetTypeMap.put("LANES", "LN")
  streetTypeMap.put("LDGE", "LDG")
  streetTypeMap.put("LIGHT", "LGT")
  streetTypeMap.put("LIGHTS", "LGTS")
  streetTypeMap.put("LNDNG", "LNDG")
  streetTypeMap.put("LOAF", "LF")
  streetTypeMap.put("LOCK", "LCK")
  streetTypeMap.put("LOCKS", "LCKS")
  streetTypeMap.put("LODG", "LDG")
  streetTypeMap.put("LODGE", "LDG")
  streetTypeMap.put("LOOPS", "LOOP")
  streetTypeMap.put("MANOR", "MNR")
  streetTypeMap.put("MANORS", "MNRS")
  streetTypeMap.put("MEADOW", "MDW")
  streetTypeMap.put("MEADOWS", "MDWS")
  streetTypeMap.put("MEDOWS", "MDWS")
  streetTypeMap.put("MILL", "ML")
  streetTypeMap.put("MILLS", "MLS")
  streetTypeMap.put("MISSION", "MSN")
  streetTypeMap.put("MISSN", "MSN")
  streetTypeMap.put("MNT", "MT")
  streetTypeMap.put("MNTAIN", "MTN")
  streetTypeMap.put("MNTN", "MTN")
  streetTypeMap.put("MNTNS", "MTNS")
  streetTypeMap.put("MOTORWAY", "MTWY")
  streetTypeMap.put("MOUNT", "MT")
  streetTypeMap.put("MOUNTAIN", "MTN")
  streetTypeMap.put("MOUNTAINS", "MTNS")
  streetTypeMap.put("MOUNTIN", "MTN")
  streetTypeMap.put("MSSN", "MSN")
  streetTypeMap.put("MTIN", "MTN")
  streetTypeMap.put("NECK", "NCK")
  streetTypeMap.put("ORCHARD", "ORCH")
  streetTypeMap.put("ORCHRD", "ORCH")
  streetTypeMap.put("OVERPASS", "OPAS")
  streetTypeMap.put("OVL", "OVAL")
  streetTypeMap.put("PARKS", "PARK")
  streetTypeMap.put("PARKWAY", "PKWY")
  streetTypeMap.put("PARKWAYS", "PKWY")
  streetTypeMap.put("PARKWY", "PKWY")
  streetTypeMap.put("PASSAGE", "PSGE")
  streetTypeMap.put("PATHS", "PATH")
  streetTypeMap.put("PIKES", "PIKE")
  streetTypeMap.put("PINE", "PNE")
  streetTypeMap.put("PINES", "PNES")
  streetTypeMap.put("PK", "PARK")
  streetTypeMap.put("PKWAY", "PKWY")
  streetTypeMap.put("PKWYS", "PKWY")
  streetTypeMap.put("PKY", "PKWY")
  streetTypeMap.put("PLACE", "PL")
  streetTypeMap.put("PLAIN", "PLN")
  streetTypeMap.put("PLAINES", "PLNS")
  streetTypeMap.put("PLAINS", "PLNS")
  streetTypeMap.put("PLAZA", "PLZ")
  streetTypeMap.put("PLZA", "PLZ")
  streetTypeMap.put("POINT", "PT")
  streetTypeMap.put("POINTS", "PTS")
  streetTypeMap.put("PORT", "PRT")
  streetTypeMap.put("PORTS", "PRTS")
  streetTypeMap.put("PRAIRIE", "PR")
  streetTypeMap.put("PRARIE", "PR")
  streetTypeMap.put("PRK", "PARK")
  streetTypeMap.put("PRR", "PR")
  streetTypeMap.put("RAD", "RADL")
  streetTypeMap.put("RADIAL", "RADL")
  streetTypeMap.put("RADIEL", "RADL")
  streetTypeMap.put("RANCH", "RNCH")
  streetTypeMap.put("RANCHES", "RNCH")
  streetTypeMap.put("RAPID", "RPD")
  streetTypeMap.put("RAPIDS", "RPDS")
  streetTypeMap.put("RDGE", "RDG")
  streetTypeMap.put("REST", "RST")
  streetTypeMap.put("RIDGE", "RDG")
  streetTypeMap.put("RIDGES", "RDGS")
  streetTypeMap.put("RIVER", "RIV")
  streetTypeMap.put("RIVR", "RIV")
  streetTypeMap.put("RNCHS", "RNCH")
  streetTypeMap.put("ROAD", "RD")
  streetTypeMap.put("ROADS", "RDS")
  streetTypeMap.put("ROUTE", "RTE")
  streetTypeMap.put("RUN", "RN")
  streetTypeMap.put("RVR", "RIV")
  streetTypeMap.put("SHOAL", "SHL")
  streetTypeMap.put("SHOALS", "SHLS")
  streetTypeMap.put("SHOAR", "SHR")
  streetTypeMap.put("SHOARS", "SHRS")
  streetTypeMap.put("SHORE", "SHR")
  streetTypeMap.put("SHORES", "SHRS")
  streetTypeMap.put("SKYWAY", "SKWY")
  streetTypeMap.put("SPNG", "SPG")
  streetTypeMap.put("SPNGS", "SPGS")
  streetTypeMap.put("SPRING", "SPG")
  streetTypeMap.put("SPRINGS", "SPGS")
  streetTypeMap.put("SPRNG", "SPG")
  streetTypeMap.put("SPRNGS", "SPGS")
  streetTypeMap.put("SPURS", "SPUR")
  streetTypeMap.put("SQR", "SQ")
  streetTypeMap.put("SQRE", "SQ")
  streetTypeMap.put("SQRS", "SQS")
  streetTypeMap.put("SQU", "SQ")
  streetTypeMap.put("SQUARE", "SQ")
  streetTypeMap.put("SQUARES", "SQS")
  streetTypeMap.put("STATION", "STA")
  streetTypeMap.put("STATN", "STA")
  streetTypeMap.put("STN", "STA")
  streetTypeMap.put("STR", "ST")
  streetTypeMap.put("STRAV", "STRA")
  streetTypeMap.put("STRAVE", "STRA")
  streetTypeMap.put("STRAVEN", "STRA")
  streetTypeMap.put("STRAVENUE", "STRA")
  streetTypeMap.put("STRAVN", "STRA")
  streetTypeMap.put("STREAM", "STRM")
  streetTypeMap.put("STREET", "ST")
  streetTypeMap.put("STREETS", "STS")
  streetTypeMap.put("STREME", "STRM")
  streetTypeMap.put("STRT", "ST")
  streetTypeMap.put("STRVN", "STRA")
  streetTypeMap.put("STRVNUE", "STRA")
  streetTypeMap.put("SUMIT", "SMT")
  streetTypeMap.put("SUMITT", "SMT")
  streetTypeMap.put("SUMMIT", "SMT")
  streetTypeMap.put("TERR", "TER")
  streetTypeMap.put("TERRACE", "TER")
  streetTypeMap.put("THROUGHWAY", "TRWY")
  streetTypeMap.put("TPK", "TPKE")
  streetTypeMap.put("TR", "TRL")
  streetTypeMap.put("TRACE", "TRCE")
  streetTypeMap.put("TRACES", "TRCE")
  streetTypeMap.put("TRACK", "TRAK")
  streetTypeMap.put("TRACKS", "TRAK")
  streetTypeMap.put("TRACKS", "TRAK")
  streetTypeMap.put("TRAFFICWAY", "TRFY")
  streetTypeMap.put("TRAIL", "TRL")
  streetTypeMap.put("TRAILS", "TRL")
  streetTypeMap.put("TRK", "TRAK")
  streetTypeMap.put("TRKS", "TRAK")
  streetTypeMap.put("TRLS", "TRL")
  streetTypeMap.put("TRNPK", "TPKE")
  streetTypeMap.put("TRPK", "TPKE")
  streetTypeMap.put("TUNEL", "TUNL")
  streetTypeMap.put("TUNLS", "TUNL")
  streetTypeMap.put("TUNNEL", "TUNL")
  streetTypeMap.put("TUNNELS", "TUNL")
  streetTypeMap.put("TUNNL", "TUNL")
  streetTypeMap.put("TURNPIKE", "TPKE")
  streetTypeMap.put("TURNPK", "TPKE")
  streetTypeMap.put("UNDERPASS", "UPAS")
  streetTypeMap.put("UNION", "UN")
  streetTypeMap.put("UNIONS", "UNS")
  streetTypeMap.put("VALLEY", "VLY")
  streetTypeMap.put("VALLEYS", "VLYS")
  streetTypeMap.put("VALLY", "VLY")
  streetTypeMap.put("VDCT", "VIA")
  streetTypeMap.put("VIADCT", "VIA")
  streetTypeMap.put("VIADUCT", "VIA")
  streetTypeMap.put("VIEW", "VW")
  streetTypeMap.put("VIEWS", "VWS")
  streetTypeMap.put("VILL", "VLG")
  streetTypeMap.put("VILLAG", "VLG")
  streetTypeMap.put("VILLAGE", "VLG")
  streetTypeMap.put("VILLAGES", "VLGS")
  streetTypeMap.put("VILLE", "VL")
  streetTypeMap.put("VILLG", "VLG")
  streetTypeMap.put("VILLIAGE", "VLG")
  streetTypeMap.put("VIST", "VIS")
  streetTypeMap.put("VISTA", "VIS")
  streetTypeMap.put("VLLY", "VLY")
  streetTypeMap.put("VST", "VIS")
  streetTypeMap.put("VSTA", "VIS")
  streetTypeMap.put("WALKS", "WALK")
  streetTypeMap.put("WELL", "WL")
  streetTypeMap.put("WELLS", "WLS")
  streetTypeMap.put("WY", "WAY")





  def main(args: Array[String]): Unit = {


    val addressCleaning = udf ((addressLine1:String,addressLine2:String) => {
      var addrs1:String = if (addressLine1 != null || addressLine1.length > 0 ) addressLine1 else ""
      val addrs2:String = if (addressLine2 != null || addressLine2.length > 0 ) addressLine2 else ""


      val numPattern = "[0-9]+".r
      val StringPattern = "[A-z]+".r

      val addrin1 = if(numPattern.findFirstIn(addrs1).getOrElse("").nonEmpty) addrs1 else addrs2
      val addrin2:String = if (addrin1.equalsIgnoreCase(addrs2)) "" else addrs2

      println(addrin1)
      println(addrin2)


      //val addrin1 = addrs1


      val match1 = numPattern.findAllIn(addrin1).toList.reverse
      val match2 = numPattern.findAllIn(addrin2).toList.reverse
      val Strmatch1 = StringPattern.findFirstIn(addrin1).getOrElse("")
      val Strmatch2 = StringPattern.findFirstIn(addrin2).getOrElse("")
      var addr1 = ""
      var addr2 = ""
      val regexp = new StringBuilder
      val streetRegex = "ST|ND|RD|TH".r
      val words = streetTypeMap.keySet



      for (word <- words) {
        regexp.append("\\b").append(word).append("\\b|")
      }
      val sencondWords = streetTypeMap.values
      for (word <- sencondWords) {
        regexp.append("\\b").append(word).append("\\b|")
      }
      val reg:Regex = regexp.toString().dropRight(1).r
      val streetType1 =  reg.findAllIn(addrin1.toUpperCase).toList
      val streetType2 =  reg.findAllIn(addressLine2.toUpperCase).toList

      val dirRegexp = new StringBuilder
      val direcWords = directionalsMap.keySet
      for (word <- direcWords) {
        dirRegexp.append("\\b").append(word).append("\\b|")
      }


      val direcWords1 = directionalsMap.values
      for (word <- direcWords1) {
        dirRegexp.append("\\b").append(word).append("\\b|")
      }

      val dirReg:Regex = dirRegexp.toString().dropRight(1).r

      val DrType1 =  dirReg.findFirstIn(addrin1.toUpperCase)
      val DrType2 =  dirReg.findFirstIn(addrin2.toUpperCase)

      if(match1.nonEmpty)
      {
        if(addrin1.indexOf("PO BOX") > -1){
          if(addrin2.indexOf("PO BOX") < 0 && match2.nonEmpty && streetType2.nonEmpty) {
            if(DrType2.nonEmpty){
              var firstInt : String = ""
              for(x <- match2){
                if(addrin2.indexOf(x) < addrin2.indexOf(DrType2))
                  firstInt = x
              }
              addr1 = addrin2.substring(addrin2.indexOf(firstInt))
            }else
              addr1 = addrin2.substring(addrin2.indexOf(match2.head))

            addr2 = addrin1.substring(addrin1.indexOf("PO BOX"))
          }
          else
            addr1 =addrin1.substring(addrin1.indexOf("PO BOX"))
        }/*else if (match1.size == 1) {
          if(streetType1.nonEmpty){
            addr1 = addrin1.substring(addrin1.indexOf(match1.head),addr1.indexOf(streetType1.head))
          }else
          addr1 = addrin1.substring(addrin1.indexOf(match1.head))
        }*/else if(DrType1.nonEmpty){
          var firstInt : String = ""
          for(x <- match1){
            if(addrin1.indexOf(x) < addrin1.indexOf(DrType1))
              firstInt = x
          }
          addr1 = addrin1.substring(addrin1.indexOf(firstInt))
        } else if (streetType1.nonEmpty){
          val streetTypeRemoval = reg.replaceAllIn(addrin1.toUpperCase,"")
          val streetType = streetRegex.replaceAllIn(streetTypeRemoval.toUpperCase,"")

          if(StringPattern.findFirstIn(streetType).getOrElse("").isEmpty){
            addr1 = addrin1.substring(addrin1.indexOf(match1.reverse.head))
          }else
          if(addrin1.indexOf(match1.head) < addrin1.indexOf(streetType1.head))
            addr1 = addrin1.substring(addrin1.indexOf(match1.head))
          else {
            addr1 = addrin1.substring(addrin1.indexOf(match1.reverse.head))
          }

        }
      }


      if (match2.nonEmpty && addr2.isEmpty   )
      {
        if(addrin2.indexOf("PO BOX") < addrin2.indexOf(numPattern.findFirstIn(addrin2).getOrElse("")) && addrin2.indexOf("PO BOX") > -1 )
        {
          if(addr1.isEmpty)
            addr1 = addrin2.substring(addrin2.indexOf("PO BOX"))
          else
            addr2 = addrin2.substring(addrin2.indexOf("PO BOX"))
        }
        else{
          if(addr1.isEmpty && streetType2.nonEmpty)
            addr1 = addrin2.substring(addrin2.indexOf(match2.head))
          else if (streetType2.nonEmpty)
            addr2 = addrin2.substring(addrin2.indexOf(match2.head))
        }
      }
      if(StringPattern.findFirstIn(addr1).getOrElse("").isEmpty)
        addr1=""
      /* if(StringPattern.findFirstIn(addr2).getOrElse("").isEmpty)
         addr2=""*/
      if (addr1.equalsIgnoreCase(addr2) || (addr1.contains(addr2) && addr2.nonEmpty))
        addr2 = ""

      if(addr1.contains(" ATT "))
        addr1 = addr1.substring(0,addr1.indexOf(" ATT "))

      if(addr1.isEmpty && addr2.nonEmpty) {
        addr1 = addr2
        addr2 = ""
      }

      if(addr1.isEmpty && addr2.isEmpty)
      {
        addr1=addressLine1
        addr2=addressLine2
      }


      addr1 + "," + addr2
    }
    )

    val add1 = ""
    val add2 = ""
    //println(addressCleaning(add1,add2))

    if(args.length != 8)
    {
      println("Required 4 params but found " + args.length)
      System.exit(1)
    }

    val dataLocation = args(0)
    val HeaderLocation = args(1)
    val addressLine1 = args(2)
    val addressLine2 = args(3)
    val dataDelimiter = args(4)
    val HeaderDelimiter = args(5)
    val HeaderInclude:Boolean = args(6).toBoolean
    val outlocation = args(7)



    if(new File(outlocation).exists())
      FileUtils.deleteDirectory(new File(outlocation))


    val sc = new SparkContext(new SparkConf())


    val sparksession = SparkSession.builder().enableHiveSupport().getOrCreate()
    import sparksession.implicits._


    val df1 = if (HeaderInclude) sparksession.read.format("csv").option("header","true").option("sep",dataDelimiter).load(dataLocation) else {
      println("Header Seperate")
      val schemaDf1 = sparksession.read.format("csv").option("header","true").option("sep",HeaderDelimiter).load(HeaderLocation).schema
      sparksession.read.format("csv").option("sep",dataDelimiter).schema(schemaDf1).load(dataLocation)
    }

    val dfColumnRenamed = df1.withColumnRenamed(addressLine1,"Addr1").withColumnRenamed(addressLine2,"addr2")

    val dfArbitration = dfColumnRenamed.withColumn("ArbAddress",addressCleaning(when('Addr1.isNull,"").otherwise($"Addr1"),when($"Addr2".isNull,"").otherwise($"Addr2")))
      .withColumn("addrLine1",expr("split(ArbAddress,',')[0] "))
      .withColumn("addrLine2",expr("split(ArbAddress,',')[1] "))
      .withColumnRenamed("Addr1",addressLine1)
      .withColumnRenamed("addr2",addressLine2)

    dfArbitration.printSchema()

    dfArbitration.write.format("csv").option("header","false").option("sep",dataDelimiter).save(outlocation+"/Data")

    //dfArbitration.limit(0).write.format("csv").option("header","true").option("sep",dataDelimiter).save(outlocation+"/Header")


    sc.parallelize(Seq(dfArbitration.columns.mkString(dataDelimiter))).coalesce(1).saveAsTextFile(outlocation+"/Header")

  }

}
