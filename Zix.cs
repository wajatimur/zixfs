using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.IO;
using System.IO.Compression;


namespace Cartella.Engine
{
    public class Zix
    {
        // Zone     : Server
        // Storage  : Set of zix lot in a folder
        // Lot      : Set of zix in a file
        // Zix      : Set of data binary and text
        // Meta     : Information on zix

        private const int  ZixSizeMax = 512;      //Maximum zix file size in MB
        private const string ZixLotFileExt = "zl";

        private ZipStorer _zixStorer;
        private ZixLot _zixLot;
        private readonly List<ZixLot> _zixLots = new List<ZixLot>();
        private readonly string _zixStorageName;
        private readonly string _zixStoragePath;


        /// <summary>
        /// 
        /// </summary>
        public Zix(string zixFolderPath, string zixStorageName)
        {
            _zixStorageName = zixStorageName;
            _zixStoragePath = zixFolderPath;

            //[i] Scan for zix lot file
            foreach(var zlFilePath in Directory.GetFiles(_zixStoragePath))
            {
                var filePart = new FileInfo(zlFilePath).Name.Split('.');
                if (filePart[1] == ZixLotFileExt)
                {
                    var zlFileSize = (int)new FileInfo(zlFilePath).Length;
                    var zlNew = new ZixLot
                                 {
                                     FileName = new FileInfo(zlFilePath).Name,
                                     FilePath = zlFilePath,
                                     FileSize = zlFileSize, 
                                     LotSequence = int.Parse( filePart[2])
                                 };
                    _zixLots.Add(zlNew);
                }
            }

            //Initialize and sync zix storage
            SyncToDisk();
        }


        /// <summary>
        /// 
        /// </summary>
        public string FileUpload(string filePath, string remoteFileName)
        {
            try
            {
               //!! add metadata
                var newZixMeta = new ZixMeta
                                     {
                                         ID = new Guid().ToString(),
                                         FileName = remoteFileName,
                                         UploadDate = DateTime.Now.ToString()
                                     };
                _zixStorer.AddFile(ZipStorer.Compression.Store, filePath, remoteFileName, "engine:db:zix");

                //[i] Sync current zix to zixlot
                _zixStorer.Close();

                //[i] Initialize and sync zix storage
                SyncToDisk();
                
                //[i] Return new created ID
                return newZixMeta.ID;
            }
            catch
            {
                return "";
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public Stream FileDownload(string fileNameKey)
        {
            Stream tStream = new MemoryStream(8192);

            //[i] Scan file on all zix lot !! should use index file
            foreach( var zlRead in _zixLots  )
            {
                ZipStorer tmpZixStorer = null ;
                if (zlRead.LotSequence == _zixLot.LotSequence)
                {
                    tmpZixStorer = _zixStorer;
                }
                else
                {
                    var zlFstream = new FileStream(_zixStoragePath + zlRead.FileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                    tmpZixStorer = ZipStorer.Open(zlFstream, FileAccess.ReadWrite);
                }

                var zixCentralDir = tmpZixStorer.ReadCentralDir();
                foreach (var zixRead in zixCentralDir)
                {
                    if (zixRead.FilenameInZip == fileNameKey)
                    {
                        tmpZixStorer.ExtractFile(zixRead, tStream);
                        return tStream;
                    }
                }
            }

            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        private void SyncToDisk()
        {

            //[i] Sync file to disk and check zix lot size
            if (_zixLots.Count > 0)
            {
                var zlLatest = _zixLots.Last();

                //[i] Need to get new file storage.
                var zlFileSize = (int)new FileInfo(zlLatest.FilePath ).Length;
                zlLatest.FileSize = zlFileSize;

                //[i] Check zix lot file size
                if (zlLatest.FileSize < (ZixSizeMax * 1024 * 1000))
                {

                    var zlFstream = new FileStream(zlLatest.FilePath,FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.ReadWrite );
                    _zixStorer = ZipStorer.Open(zlFstream, FileAccess.ReadWrite);
                    _zixLot = zlLatest;
                }
                else
                {
                    var zlNewSeq = _zixLots.Count + 1;
                    var zlNewFileName = _zixStorageName + "." + ZixLotFileExt + "." + zlNewSeq ;
                    var zlNew = new ZixLot
                    {
                        FileName = zlNewFileName,
                        FilePath = _zixStoragePath + zlNewFileName,
                        FileSize = 0,
                        LotSequence = _zixLots.Count + 1
                    };

                    _zixStorer = ZipStorer.Create(_zixStoragePath + zlNew.FileName, "engine:db:zix");
                    _zixLots.Add(zlNew);
                    _zixLot = zlNew;
                }

            }
            else
            {
                var zlNewFileName = _zixStorageName + "." + ZixLotFileExt + ".1";
                var zlNew = new ZixLot
                {
                    FileName = zlNewFileName,
                    FilePath = _zixStoragePath + zlNewFileName,
                    FileSize = 0,
                    LotSequence = 1
                };

                _zixStorer = ZipStorer.Create(zlNew.FilePath, "engine:db:zix");
                _zixLots.Add(zlNew);
                _zixLot = zlNew;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public ZixLot ZixLot
        {
            get { return _zixLot; }
        }


        /// <summary>
        /// 
        /// </summary>
        ~Zix()
        {
            if (_zixStorer != null) _zixStorer.Close();
        }
    }


    public class ZixLot
    {
        public string FileName { get; set; }
        public string FilePath { get; set; }
        public int LotSequence { get; set; }
        public int FileSize { get; set; }
    }


    public class ZixMeta
    {
        public string ID { get; set; }
        public string UploadDate { get; set; }
        public string MD5 { get; set; }
        public string FileName { get; set; }
        public string ContentType { get; set; }
    }
}
