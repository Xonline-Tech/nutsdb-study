package nutsdb

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var BucketMetaSize int64

const (
	IdSize = 8
	DsSize = 2
)

type BucketOperation uint16

const (
	BucketInsertOperation BucketOperation = 1
	BucketUpdateOperation BucketOperation = 2
	BucketDeleteOperation BucketOperation = 3
)

var ErrBucketCrcInvalid = errors.New("bucket crc invalid")

func init() {
	BucketMetaSize = GetDiskSizeFromSingleObject(BucketMeta{})
}

// BucketMeta 存储存储桶的元信息。例如，它存储在磁盘中的存储桶的大小。
type BucketMeta struct {
	Crc uint32
	// Op: 标记此存储桶的最新操作（例如删除、插入、更新）。
	Op BucketOperation
	// Size: 有效负载的大小。
	Size uint32
}

// Bucket 是存储桶的磁盘结构
type Bucket struct {
	// Meta: 此存储桶的元数据
	Meta *BucketMeta
	// Id: 是此存储桶的标记，则每个存储桶创建活动都会为其生成一个新的 ID。
	// 例如。如果您有一个名为“bucket_1”的存储桶，并且您只需删除存储桶并重新创建它。
	// 最后一个存储桶的 ID 将与前一个存储桶不同。
	Id BucketId
	// Ds: 此存储桶的数据结构。（列表、 集、 排序集、 字符串）
	Ds Ds
	// Name: 此存储桶的名称。
	Name string
}

// Decode : CRC | op | size
func (meta *BucketMeta) Decode(bytes []byte) {
	_ = bytes[BucketMetaSize-1]
	crc := binary.LittleEndian.Uint32(bytes[:4])
	op := binary.LittleEndian.Uint16(bytes[4:6])
	size := binary.LittleEndian.Uint32(bytes[6:10])
	meta.Crc = crc
	meta.Size = size
	meta.Op = BucketOperation(op)
}

// Encode : Meta | BucketId | Ds | BucketName
func (b *Bucket) Encode() []byte {
	entrySize := b.GetEntrySize()
	buf := make([]byte, entrySize)
	b.Meta.Size = uint32(b.GetPayloadSize())
	binary.LittleEndian.PutUint16(buf[4:6], uint16(b.Meta.Op))
	binary.LittleEndian.PutUint32(buf[6:10], b.Meta.Size)
	binary.LittleEndian.PutUint64(buf[BucketMetaSize:BucketMetaSize+IdSize], uint64(b.Id))
	binary.LittleEndian.PutUint16(buf[BucketMetaSize+IdSize:BucketMetaSize+IdSize+DsSize], uint16(b.Ds))
	copy(buf[BucketMetaSize+IdSize+DsSize:], b.Name)
	c32 := crc32.ChecksumIEEE(buf[4:])
	b.Meta.Crc = c32
	binary.LittleEndian.PutUint32(buf[0:4], c32)

	return buf
}

// Decode : Meta | BucketId | Ds | BucketName
func (b *Bucket) Decode(bytes []byte) error {
	// parse the payload
	id := binary.LittleEndian.Uint64(bytes[:IdSize])
	ds := binary.LittleEndian.Uint16(bytes[IdSize : IdSize+DsSize])
	name := bytes[IdSize+DsSize:]
	b.Id = id
	b.Name = string(name)
	b.Ds = ds
	return nil
}

func (b *Bucket) GetEntrySize() int {
	return int(BucketMetaSize) + b.GetPayloadSize()
}

func (b *Bucket) GetCRC(headerBuf []byte, dataBuf []byte) uint32 {
	crc := crc32.ChecksumIEEE(headerBuf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, dataBuf)
	return crc
}

func (b *Bucket) GetPayloadSize() int {
	return IdSize + DsSize + len(b.Name)
}
