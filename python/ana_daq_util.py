import math

def check_counts_offsets(counts, offsets, total):    
    '''Makes sure that the counts and offsets partition total. 
    
    Throws an exception if there is a problem

    Examples:
      >>> check_counts_offsets(counts=[2,2,2], offsets=[0,2,4], total=6)
      # this is correct. 
      >>> check_counts_offsets(counts=[2,2,2], offsets=[0,2,4], total=7)
      # incorrect, throws assert
      >>> check_counts_offsets(counts=[2,2,2], offsets=[2,4,6], total=6)
      # incorrect, ect
    '''
    assert sum(counts)==total, 'counts=%r offsets=%r do not partition total=%d' % (counts, offsets, total)
    assert offsets[0]==0, 'counts=%r offsets=%r do not partition total=%d' % (counts, offsets, total)
    assert len(counts)==len(offsets), 'counts=%r offsets=%r do not partition total=%d' % (counts, offsets, total)
    for i in range(1,len(counts)):
        assert offsets[i]==offsets[i-1]+counts[i-1], 'counts=%r offsets=%r do not partition total=%d' % (counts, offsets, total)
    assert offsets[-1]+counts[-1]==total, 'counts=%r offsets=%r do not partition total=%d' % (counts, offsets, total)

def divide_evenly(total, splits):
    '''partition the amount of data as evenly as possible.

    Examples: 
      >>> divide_evenly(11,3) 
      returns offsets=[0,4,8]
              counts=[4,4,3]    
    '''
    assert splits > 0, "divide_evenly - splits is <= 0"
    k = int(math.floor(total / splits))
    r = total % splits
    offsets=[]
    counts=[]
    nextOffset = 0
    for w in range(splits):
        offsets.append(nextOffset)
        count = k
        if r > 0: 
            count += 1
            r -= 1
        counts.append(count)
        nextOffset += count
    check_counts_offsets(counts, offsets, total)
    return offsets, counts            
