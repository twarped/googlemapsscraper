(() => {
    if (document.querySelector('#popup button[aria-label=Dismiss]')) {
        return 'popup';
    }
    if (document.getElementById('google-hats-survey')) {
        return 'survey';
    }
    if (document.querySelector('#interactive-hovercard')?.style.display == 'block') {
        return 'hovercard';
    }

    let feed = document.querySelector('[role=feed]');
    let feedChildren = feed?.children;
    let lastFeedChildChildren = feedChildren?.[feedChildren.length - 1]?.children;
    let feedScrolledToBottom = feed?.scrollHeight - feed?.scrollTop - 1 < feed?.clientHeight;
    let hasFeed = feedChildren && lastFeedChildChildren;
    let refreshButton = document.querySelector('[jsaction=\"search.refresh\"]');
    let refreshButtonRect = refreshButton?.getBoundingClientRect();
    let refreshLoadingIcon = refreshButton?.querySelector('span[aria-hidden]');
    let hasResults = lastFeedChildChildren?.length > 0;
    let isEnd = document.querySelector('[role=feed]>div:last-child>div>p') || (hasFeed && !hasResults);
    let feedCount = [...(feed?.querySelectorAll('a.hfpxzc') || [])].length;
    let searchBoxClassCount = document.querySelector('#omnibox-singlebox')?.classList.length;
    let isLoading = (searchBoxClassCount || (refreshButton && !refreshLoadingIcon)) || (hasResults && feedScrolledToBottom && !isEnd);
    if (document.querySelector('[role=main]:not(:has([role=feed]))') && hasFeed) {
        return 'place-open';
    }
    if (isLoading) {
        return 'loading';
    }
    if (refreshButtonRect?.top > 0 && refreshButtonRect?.left > 0 && refreshLoadingIcon) {
        return 'refresh';
    }
    if (!feedScrolledToBottom && feedChildren && !isEnd) {
        return 'scroll';
    }
    if (!hasFeed) {
        return 'no-feed';
    }
    if (isEnd) {
        return 'end|' + feedCount;
    }
})();