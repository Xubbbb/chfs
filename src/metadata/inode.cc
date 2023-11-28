#include "metadata/inode.h"

namespace chfs {

auto Inode::begin() -> InodeIterator { return InodeIterator(this, 0); }

auto Inode::end() -> InodeIterator {
  return InodeIterator(this, this->nblocks);
}

auto Inode::write_indirect_block(std::shared_ptr<BlockManager> &bm,
                                 std::vector<u8> &buffer) -> ChfsNullResult {
  if (this->blocks[this->nblocks - 1] == KInvalidBlockID) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  return bm->write_block(this->blocks[this->nblocks - 1], buffer.data());
}

//[ transaction ]//
auto Inode::write_indirect_block_atomic(std::shared_ptr<BlockManager> &bm,
                            std::vector<u8> &buffer, std::vector<std::shared_ptr<BlockOperation>> &tx_ops) -> ChfsNullResult{
  if (this->blocks[this->nblocks - 1] == KInvalidBlockID) {
    return ChfsNullResult(ErrorType::INVALID_ARG);
  }

  return bm->write_block_to_memory(this->blocks[this->nblocks - 1], buffer.data(), tx_ops);
}
//[ transaction ]//

} // namespace chfs